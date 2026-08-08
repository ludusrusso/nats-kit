package natsjs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs"
	"github.com/ludusrusso/nats-cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

// waitForConsumer polls until a durable consumer named durable exists on
// stream, or fails the test after timeout. Tests that publish an Event
// need this: with DeliverNewPolicy, a consumer only ever sees what is
// published after it exists, so publishing before every Handler's
// consumer has actually been created server-side would be a race in the
// test itself, not a bug in the Runner being tested.
func waitForConsumer(t *testing.T, nc *nats.Conn, stream, durable string) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	waitUntil(t, 5*time.Second, "durable consumer "+durable+" exists on "+stream, func() bool {
		_, err := js.Consumer(context.Background(), stream, durable)
		return err == nil
	})
}

func TestRunner_CommandDeliveredExactlyOnceToItsHandler(t *testing.T) {
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

	runner, err := natsjs.New(ctx, nc, "orders-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	bus := natscqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-1"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 5*time.Second, "handler receives the command", func() bool {
		return got.len() == 1
	})

	staysAt(t, 300*time.Millisecond, "handler is invoked exactly once", func() bool {
		return got.len() == 1
	})

	items := got.snapshot()
	if items[0].CustomerID != "cust-1" {
		t.Fatalf("got CustomerID %q, want %q", items[0].CustomerID, "cust-1")
	}
}

func TestRunner_EventFansOutToEveryRegisteredHandlerIndependently(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	gotA := &collector[testOrderCreated]{}
	handlerA := natscqrs.NewEventHandler("notify_shipping", func(_ context.Context, evt testOrderCreated) error {
		gotA.add(evt)
		return nil
	})
	gotB := &collector[testOrderCreated]{}
	handlerB := natscqrs.NewEventHandler("notify_billing", func(_ context.Context, evt testOrderCreated) error {
		gotB.add(evt)
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "notifications-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handlerA, handlerB); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	// Both durable consumers must exist before publishing: DeliverNewPolicy
	// means a consumer only sees what is published after it was created,
	// so publishing any earlier would race the Runner's own startup.
	waitForConsumer(t, nc, natsjs.EventsStreamName, "notifications-svc_notify_shipping")
	waitForConsumer(t, nc, natsjs.EventsStreamName, "notifications-svc_notify_billing")

	bus := natscqrs.NewEventBus(pub)
	if err := bus.Publish(ctx, testOrderCreated{OrderID: "order-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitUntil(t, 5*time.Second, "handler A receives the event", func() bool { return gotA.len() == 1 })
	waitUntil(t, 5*time.Second, "handler B receives the event", func() bool { return gotB.len() == 1 })

	staysAt(t, 300*time.Millisecond, "each handler is invoked exactly once", func() bool {
		return gotA.len() == 1 && gotB.len() == 1
	})

	if gotA.snapshot()[0].OrderID != "order-1" || gotB.snapshot()[0].OrderID != "order-1" {
		t.Fatalf("unexpected payload delivered")
	}
}

func TestRunner_TwoRunnersWithSameServiceAndHandlerNameCompeteForACommand(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	got := &collector[testCreateOrder]{}
	newHandler := func() natscqrs.Handler {
		return natscqrs.NewCommandHandler("create_order", func(_ context.Context, cmd testCreateOrder) error {
			got.add(cmd)
			return nil
		})
	}

	runnerA, err := natsjs.New(ctx, nc, "orders-svc")
	if err != nil {
		t.Fatalf("New (A): %v", err)
	}
	if err := runnerA.Register(newHandler()); err != nil {
		t.Fatalf("Register (A): %v", err)
	}
	stopA := runInBackground(t, runnerA)
	defer stopA()

	runnerB, err := natsjs.New(ctx, nc, "orders-svc")
	if err != nil {
		t.Fatalf("New (B): %v", err)
	}
	if err := runnerB.Register(newHandler()); err != nil {
		t.Fatalf("Register (B): %v", err)
	}
	stopB := runInBackground(t, runnerB)
	defer stopB()

	bus := natscqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-2"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 5*time.Second, "exactly one replica handles the command", func() bool {
		return got.len() == 1
	})

	staysAt(t, 300*time.Millisecond, "the command is handled exactly once, not twice", func() bool {
		return got.len() == 1
	})
}

func TestRunner_ConsumerCreatedAfterEventsWerePublishedDoesNotReceiveThem(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	bus := natscqrs.NewEventBus(pub)
	// Published before any consumer for this Handler exists: this is
	// history the Handler is not owed (ADR 0001).
	if err := bus.Publish(ctx,
		testOrderCreated{OrderID: "history-1"},
		testOrderCreated{OrderID: "history-2"},
	); err != nil {
		t.Fatalf("Publish (history): %v", err)
	}

	got := &collector[testOrderCreated]{}
	handler := natscqrs.NewEventHandler("late_subscriber", func(_ context.Context, evt testOrderCreated) error {
		got.add(evt)
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "late-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	// Wait for the durable consumer to actually exist server-side before
	// publishing "fresh-1" below, instead of relying on staysAt's window as
	// an implicit timer: on a slow or loaded machine, staysAt's 500ms is not
	// guaranteed to be enough time for CreateOrUpdateConsumer to have
	// completed, and publishing before it has would make this test's own
	// setup race the Runner it is testing — "fresh-1" would itself become
	// history a not-yet-existing DeliverNew consumer is not owed, and the
	// waitUntil below would time out for a reason that has nothing to do
	// with the Runner's correctness.
	waitForConsumer(t, nc, natsjs.EventsStreamName, "late-svc_late_subscriber")

	staysAt(t, 300*time.Millisecond, "no history is delivered to a newly created consumer", func() bool {
		return got.len() == 0
	})

	if err := bus.Publish(ctx, testOrderCreated{OrderID: "fresh-1"}); err != nil {
		t.Fatalf("Publish (fresh): %v", err)
	}

	waitUntil(t, 5*time.Second, "a new event published after the consumer exists is delivered", func() bool {
		return got.len() == 1
	})
	if got.snapshot()[0].OrderID != "fresh-1" {
		t.Fatalf("got OrderID %q, want %q", got.snapshot()[0].OrderID, "fresh-1")
	}
}

func TestRunner_RejectsInvalidServiceAndHandlerNames(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := natsjs.New(ctx, nc, "bad.name"); err == nil {
		t.Fatalf("New: want error for a service name containing '.', got nil")
	}
	if _, err := natsjs.New(ctx, nc, "bad name"); err == nil {
		t.Fatalf("New: want error for a service name containing whitespace, got nil")
	}
	if _, err := natsjs.New(ctx, nc, ""); err == nil {
		t.Fatalf("New: want error for an empty service name, got nil")
	}

	runner, err := natsjs.New(ctx, nc, "good-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	badHandler := natscqrs.NewCommandHandler("bad>name", func(_ context.Context, _ testCreateOrder) error { return nil })
	if err := runner.Register(badHandler); err == nil {
		t.Fatalf("Register: want error for a handler name containing '>', got nil")
	}
}

// TestNew_RejectsMaxDeliverLessThanOne is FIX 5's test. To JetStream,
// MaxDeliver <= 0 means "unlimited attempts" — the opposite of what it
// looks like it means here. Left unchecked, WithMaxDeliver(0) (or a
// negative value) makes handleMessage's own "attempts >= maxDeliver" check
// true on the very first delivery, dead-lettering a Handler's first
// failure instead of ever retrying it — and it used to surface, if at
// all, only once Run actually tried to create a consumer with it, not at
// New. New must reject it immediately instead.
func TestNew_RejectsMaxDeliverLessThanOne(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	for _, n := range []int{0, -1, -100} {
		if _, err := natsjs.New(ctx, nc, "bad-maxdeliver-svc", natsjs.WithMaxDeliver(n)); err == nil {
			t.Errorf("New: want error for WithMaxDeliver(%d), got nil", n)
		}
	}

	if _, err := natsjs.New(ctx, nc, "good-maxdeliver-svc", natsjs.WithMaxDeliver(1)); err != nil {
		t.Errorf("New: WithMaxDeliver(1) should be accepted, got %v", err)
	}
}

func TestRunner_RegisterRejectsDuplicateHandlerNames(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	runner, err := natsjs.New(ctx, nc, "dup-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h1 := natscqrs.NewCommandHandler("create_order", func(_ context.Context, _ testCreateOrder) error { return nil })
	h2 := natscqrs.NewCommandHandler("create_order", func(_ context.Context, _ testCreateOrder) error { return nil })
	if err := runner.Register(h1); err != nil {
		t.Fatalf("Register (first): %v", err)
	}
	if err := runner.Register(h2); err == nil {
		t.Fatalf("Register (duplicate): want error, got nil")
	}
}

// TestRunner_ConsumersEffectiveAckWaitMatchesWithAckWait is FIX 2's test.
// Before BackOff was deleted from this package, nats-server would silently
// substitute BackOff's first interval for AckWait whenever BackOff was
// non-empty (config.AckWait = config.BackOff[0], unconditionally — see
// nats-server's server/consumer.go): DefaultBackOff meant the consumer's
// real AckWait was always 10s regardless of WithAckWait, which was
// therefore a no-op by default. This asserts the consumer JetStream
// actually created has the AckWait this Runner asked for, not some other
// value substituted underneath it.
func TestRunner_ConsumersEffectiveAckWaitMatchesWithAckWait(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const wantAckWait = 7 * time.Second

	handler := natscqrs.NewCommandHandler("ackwait_check", func(_ context.Context, _ testCreateOrder) error { return nil })

	runner, err := natsjs.New(ctx, nc, "ackwait-svc", natsjs.WithAckWait(wantAckWait))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	waitForConsumer(t, nc, natsjs.CommandsStreamName, "ackwait-svc_ackwait_check")

	cons, err := js.Consumer(ctx, natsjs.CommandsStreamName, "ackwait-svc_ackwait_check")
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.Config.AckWait != wantAckWait {
		t.Errorf("consumer AckWait = %v, want %v (the value WithAckWait requested)", info.Config.AckWait, wantAckWait)
	}
	if len(info.Config.BackOff) != 0 {
		t.Errorf("consumer BackOff = %v, want empty: this package no longer configures one", info.Config.BackOff)
	}
}

// errAlwaysFails is returned by a Handler that never succeeds, used by the
// Dead Letter tests in deadletter_test.go, but declared here since it is a
// trivial, shared fixture.
var errAlwaysFails = errors.New("handler: intentional failure for test purposes")
