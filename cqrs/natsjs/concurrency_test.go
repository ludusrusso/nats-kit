package natsjs_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ludusrusso/nats-cqrs/cqrs"
	"github.com/ludusrusso/nats-cqrs/cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

func TestRunner_DefaultConcurrencyIsExactlyOnePerHandler(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	const total = 5
	var inFlight, maxObserved atomic.Int32
	done := &collector[string]{}

	handler := cqrs.NewCommandHandler("solo_process", func(_ context.Context, cmd testCreateOrder) error {
		n := inFlight.Add(1)
		for {
			old := maxObserved.Load()
			if n <= old || maxObserved.CompareAndSwap(old, n) {
				break
			}
		}
		// Give a (hypothetically buggy) concurrent invocation a chance to
		// overlap with this one before it finishes.
		time.Sleep(30 * time.Millisecond)
		inFlight.Add(-1)
		done.add(cmd.CustomerID)
		return nil
	})

	// No WithHandlerWorkers: DefaultWorkers (one) applies.
	runner, err := natsjs.New(ctx, nc, "solo-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	bus := cqrs.NewCommandBus(pub)
	for i := 0; i < total; i++ {
		if err := bus.Send(ctx, testCreateOrder{CustomerID: fmt.Sprintf("cust-%d", i)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	waitUntil(t, 10*time.Second, "all commands are handled", func() bool {
		return done.len() == total
	})

	if got := maxObserved.Load(); got != 1 {
		t.Errorf("observed %d handler invocations in flight at once, want exactly 1 (the default)", got)
	}
}

func TestRunner_WithHandlerWorkersRaisesConcurrencyForThatHandlerOnly(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	const workers = 3
	var inFlight atomic.Int32
	release := make(chan struct{})

	handler := cqrs.NewCommandHandler("bulk_process", func(_ context.Context, _ testCreateOrder) error {
		n := inFlight.Add(1)
		if n > int32(workers) {
			t.Errorf("%d handler invocations in flight at once, want at most %d", n, workers)
		}
		<-release
		inFlight.Add(-1)
		return nil
	})

	// A second Handler, registered on the same Runner with no
	// WithHandlerWorkers override of its own: "ForThatHandlerOnly" in this
	// test's name is only actually asserted if something else exists that
	// the override could have leaked into but didn't. Without this Handler,
	// raising bulk_process's concurrency to 3 and this one staying at
	// DefaultWorkers (1) look identical from the test's point of view.
	//
	// It consumes a different Message kind (an Event, not a Command) on
	// purpose: two Command Handlers built from the very same Command type
	// would both filter on the identical subject, which the commands
	// stream's work-queue retention rejects outright as an overlapping
	// filter (see durableName's doc comment) — a real constraint, but one
	// that has nothing to do with what this test is checking.
	const otherTotal = 5
	var otherInFlight, otherMaxObserved atomic.Int32
	otherDone := &collector[string]{}
	other := cqrs.NewEventHandler("solo_process", func(_ context.Context, evt testOrderCreated) error {
		n := otherInFlight.Add(1)
		for {
			old := otherMaxObserved.Load()
			if n <= old || otherMaxObserved.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		otherInFlight.Add(-1)
		otherDone.add(evt.OrderID)
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "bulk-svc", natsjs.WithHandlerWorkers("bulk_process", workers))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler, other); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	// other is an Event Handler with DeliverNewPolicy: its consumer must
	// exist before publishing, or these Events become history it is not
	// owed (ADR 0001) and otherDone would never reach otherTotal.
	waitForConsumer(t, nc, natsjs.EventsStreamName, "bulk-svc_solo_process")

	cmdBus := cqrs.NewCommandBus(pub)
	for i := 0; i < workers; i++ {
		if err := cmdBus.Send(ctx, testCreateOrder{CustomerID: fmt.Sprintf("cust-%d", i)}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	evtBus := cqrs.NewEventBus(pub)
	for i := 0; i < otherTotal; i++ {
		if err := evtBus.Publish(ctx, testOrderCreated{OrderID: fmt.Sprintf("other-%d", i)}); err != nil {
			t.Fatalf("Publish (other): %v", err)
		}
	}

	waitUntil(t, 10*time.Second, "workers handler invocations are concurrently in flight", func() bool {
		return inFlight.Load() == int32(workers)
	})

	waitUntil(t, 10*time.Second, "the un-overridden handler finishes all its events", func() bool {
		return otherDone.len() == otherTotal
	})
	if got := otherMaxObserved.Load(); got != 1 {
		t.Errorf("the un-overridden handler observed %d invocations in flight at once, want exactly 1: "+
			"WithHandlerWorkers for a different handler name must not affect it", got)
	}

	close(release)

	waitUntil(t, 5*time.Second, "every released invocation completes", func() bool {
		return inFlight.Load() == 0
	})
}
