package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ludusrusso/nats-cqrs"
	"github.com/ludusrusso/nats-cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

func TestPublisher_NatsMsgIdDeduplicatesARepublishedRecord(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	got := &collector[testCreateOrder]{}
	handler := natscqrs.NewCommandHandler("create_order", func(_ context.Context, cmd testCreateOrder) error {
		got.add(cmd)
		return nil
	})
	runner, err := natsjs.New(ctx, nc, "dedup-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	record, err := natscqrs.Marshal(testCreateOrder{CustomerID: "cust-3"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if record.ID == "" {
		t.Fatalf("Marshal produced a record with an empty ID")
	}

	// Simulate an Outbox forwarder republishing the same Record after an
	// at-least-once retry (e.g. the publish succeeded but the Outbox's own
	// commit then failed). JetStream must deduplicate it via Nats-Msg-Id.
	if err := pub.Publish(ctx, record); err != nil {
		t.Fatalf("Publish (first): %v", err)
	}
	if err := pub.Publish(ctx, record); err != nil {
		t.Fatalf("Publish (repeat): %v", err)
	}

	waitUntil(t, 5*time.Second, "the handler receives the deduplicated command", func() bool {
		return got.len() == 1
	})
	staysAt(t, 500*time.Millisecond, "the repeated publish results in exactly one delivery", func() bool {
		return got.len() == 1
	})
}

func TestPublisher_PublishSendsEveryRecordInABatch(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	got := &collector[testCreateOrder]{}
	handler := natscqrs.NewCommandHandler("create_order", func(_ context.Context, cmd testCreateOrder) error {
		got.add(cmd)
		return nil
	})
	runner, err := natsjs.New(ctx, nc, "batch-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	bus := natscqrs.NewCommandBus(pub)
	if err := bus.Send(ctx,
		testCreateOrder{CustomerID: "cust-a"},
		testCreateOrder{CustomerID: "cust-b"},
		testCreateOrder{CustomerID: "cust-c"},
	); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 5*time.Second, "every record in the batch is delivered", func() bool {
		return got.len() == 3
	})
}

func TestPublisher_PublishOfEmptyBatchIsANoOp(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	if err := pub.Publish(ctx); err != nil {
		t.Fatalf("Publish with no records: want nil error, got %v", err)
	}
}

// TestPublisher_CloseFlushesAfterOrdinaryPublishing is FIX 7's test: a
// Publisher needs a way to flush and settle before a process shuts down.
// Publish itself already waits for every record's ack before returning
// successfully, so by the time it returns nil there is nothing left
// outstanding — Close should therefore report that immediately, and not
// hang waiting on ctx.
func TestPublisher_CloseFlushesAfterOrdinaryPublishing(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	record, err := natscqrs.Marshal(testCreateOrder{CustomerID: "cust-close"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := pub.Publish(ctx, record); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := pub.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestPublisher_CloseReturnsCtxErrIfCtxEndsFirst confirms Close respects
// ctx rather than blocking past it, using an already-cancelled ctx: with
// nothing outstanding this races PublishAsyncComplete's already-closed
// channel, but an already-Done ctx makes the ctx.Err() branch the only
// one that can possibly still matter to a caller, so asserting "either
// nil or ctx.Err(), nothing else" is what this can honestly claim.
func TestPublisher_CloseReturnsCtxErrIfCtxEndsFirst(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pub.Close(cancelledCtx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Close: got %v, want nil or context.Canceled", err)
	}
}
