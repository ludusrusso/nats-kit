package natsjs_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ludusrusso/nats-kit/cqrs"
	"github.com/ludusrusso/nats-kit/cqrs/natsjs"
	"github.com/ludusrusso/nats-kit/natstest"
)

// TestRunner_PanickingHandlerIsRetriedThenDeadLetteredWithoutKillingTheRunner
// is FIX 1's test: a Handler function that panics must not propagate past
// handleMessage and kill the process. Before the fix, there was no
// recover() anywhere on this path, so a panic inside a Handler would
// unwind straight through runHandler and Run, taking down every other
// Handler — and every other Runner — in the same process, with the
// in-flight message neither acked nor naked.
//
// This test proves the opposite: the panicking Handler is retried up to
// MaxDeliver times (exactly like a Handler that returns an ordinary
// error), is eventually dead-lettered, and — critically — the Runner
// itself, and a second, perfectly healthy Handler running on it, are
// never affected.
func TestRunner_PanickingHandlerIsRetriedThenDeadLetteredWithoutKillingTheRunner(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const maxDeliver = 3

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	dlq := &collector[cqrs.DeadLetter]{}
	unmarshalErrs := make(chan error, 8)
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if err := json.Unmarshal(msg.Data, &dl); err != nil {
			select {
			case unmarshalErrs <- err:
			default:
			}
			return
		}
		dlq.add(dl)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	var panicking atomic.Int32
	panicker := cqrs.NewCommandHandler("panics_every_time", func(_ context.Context, _ testCreateOrder) error {
		panicking.Add(1)
		panic("boom: this handler always panics")
	})

	// A second, healthy Handler on the very same Runner: proof that the
	// panic is contained to the one Handler invocation that caused it, not
	// merely "does not crash the test binary" (which a panic recovered by
	// the Go test framework's own machinery could arguably also achieve).
	// It consumes a different Message kind (an Event, not a Command) on
	// purpose: a second Command Handler built from the same Command type
	// would collide on the commands work-queue stream's filter-subject
	// uniqueness constraint (see durableName's doc comment) — a real but
	// unrelated failure mode this test does not want to trip over.
	healthy := &collector[testOrderCreated]{}
	healthyHandler := cqrs.NewEventHandler("stays_healthy", func(_ context.Context, evt testOrderCreated) error {
		healthy.add(evt)
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "panic-svc",
		natsjs.WithMaxDeliver(maxDeliver),
		natsjs.WithAckWait(150*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(panicker, healthyHandler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	// healthyHandler is an Event Handler with DeliverNewPolicy: its
	// consumer must exist before publishing, or the Event below becomes
	// history it is not owed (ADR 0001) and the final assertion would time
	// out for a reason unrelated to what this test checks.
	waitForConsumer(t, nc, natsjs.EventsStreamName, "panic-svc_stays_healthy")

	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-panics"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 10*time.Second, "the panicking handler is retried until it is dead-lettered", func() bool {
		return dlq.len() == 1
	})

	select {
	case err := <-unmarshalErrs:
		t.Fatalf("received a malformed dead letter: %v", err)
	default:
	}

	dl := dlq.snapshot()[0]
	if dl.Handler != "panics_every_time" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "panics_every_time")
	}
	if dl.Attempts != maxDeliver {
		t.Errorf("DeadLetter.Attempts = %d, want %d", dl.Attempts, maxDeliver)
	}
	if dl.Error == "" {
		t.Errorf("DeadLetter.Error is empty, want it to carry the panic value and a stack trace")
	}

	if got := int(panicking.Load()); got != maxDeliver {
		t.Errorf("panicking handler was invoked %d times, want %d", got, maxDeliver)
	}

	// The original message must have stopped cycling, exactly like an
	// ordinary dead-lettered failure.
	staysAt(t, 300*time.Millisecond, "the panicking handler is not invoked again after being dead-lettered", func() bool {
		return int(panicking.Load()) == maxDeliver && dlq.len() == 1
	})

	// The Runner itself, and its other, healthy Handler, are unaffected:
	// publish an Event through the healthy Handler and confirm it is
	// processed normally, proving the panic never propagated past
	// handleMessage and brought anything else down with it.
	evtBus := cqrs.NewEventBus(pub)
	if err := evtBus.Publish(ctx, testOrderCreated{OrderID: "order-healthy"}); err != nil {
		t.Fatalf("Publish (healthy): %v", err)
	}
	waitUntil(t, 5*time.Second, "the healthy handler on the same runner still works", func() bool {
		return healthy.len() == 1
	})
}
