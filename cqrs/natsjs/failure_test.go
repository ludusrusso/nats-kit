package natsjs_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-kit/cqrs"
	"github.com/ludusrusso/nats-kit/cqrs/natsjs"
	"github.com/ludusrusso/nats-kit/natstest"
)

// TestRunner_ConsumerDeletedMidRunSurfacesAsAnErrorFromRun is part of FIX
// 3's test coverage. Before the fix, runHandler broke its loop on any
// iter.Next error (logging it, unless it was ErrMsgIteratorClosed) and
// then returned nil regardless; Run joined only nils and returned nil. A
// durable consumer deleted out from under a running Handler — Next
// returns jetstream.ErrConsumerDeleted, a terminal error — used to be
// reported as a perfectly clean shutdown.
func TestRunner_ConsumerDeletedMidRunSurfacesAsAnErrorFromRun(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	handler := cqrs.NewCommandHandler("solo_process", func(_ context.Context, _ testCreateOrder) error { return nil })

	runner, err := natsjs.New(ctx, nc, "deleted-consumer-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()

	const durable = "deleted-consumer-svc_solo_process"
	waitForConsumer(t, nc, natsjs.CommandsStreamName, durable)

	if err := js.DeleteConsumer(context.Background(), natsjs.CommandsStreamName, durable); err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Run: want a non-nil error once its consumer is deleted server-side, got nil")
		}
		t.Logf("Run correctly reported: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return within the timeout after its consumer was deleted; " +
			"the pre-fix behavior was to silently stop this Handler and return nil")
	}
}

// TestRunner_MalformedEnvelopeThatIsValidJSONIsDeadLetteredWithoutCrashingTheRunner
// is FIX 9's transport-failure coverage for the realistic shape of
// "malformed data reaching a live Runner": syntactically valid JSON that
// is not the envelope this package itself always produces — e.g. wrong
// schema, wrong Message Name, a hand-crafted or corrupted-but-still-JSON
// payload. Handle's own error path (envelope/payload unmarshal failure)
// already runs through the ordinary nak/dead-letter machinery like any
// other Handler error; this proves it end to end, and that the Runner and
// this same Handler keep working for whatever is published afterward.
func TestRunner_MalformedEnvelopeThatIsValidJSONIsDeadLetteredWithoutCrashingTheRunner(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsjs.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	var invocations atomic.Int32
	handler := cqrs.NewCommandHandler("create_order", func(_ context.Context, _ testCreateOrder) error {
		invocations.Add(1)
		return nil
	})

	dlqCh := make(chan cqrs.DeadLetter, 1)
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if json.Unmarshal(msg.Data, &dl) == nil {
			select {
			case dlqCh <- dl:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	runner, err := natsjs.New(ctx, nc, "malformed-svc",
		natsjs.WithMaxDeliver(2),
		natsjs.WithAckWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	waitForConsumer(t, nc, natsjs.CommandsStreamName, "malformed-svc_create_order")

	// Valid JSON, reaches the Handler on exactly the subject it is
	// subscribed to like a well-formed Command would, but its "name" does
	// not match what this Handler expects — Handle rejects it before ever
	// invoking the registered function.
	malformed := []byte(`{"name":"not_the_expected_message_name","payload":{}}`)
	if _, err := js.Publish(ctx, handler.Subject(), malformed); err != nil {
		t.Fatalf("Publish (malformed): %v", err)
	}

	var dl cqrs.DeadLetter
	select {
	case dl = <-dlqCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for the malformed message to be dead-lettered")
	}
	if dl.Handler != "create_order" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "create_order")
	}
	if invocations.Load() != 0 {
		t.Errorf("handler function ran %d time(s); a malformed envelope must never reach the registered function", invocations.Load())
	}

	// The Runner itself, and this same Handler, must still be alive: send
	// a well-formed Command through afterward and confirm it is handled
	// normally.
	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-after-malformed"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitUntil(t, 5*time.Second, "the runner keeps serving its handler after a malformed message", func() bool {
		return invocations.Load() == 1
	})
}

// TestRunner_InvalidJSONSyntaxIsNakedWithoutCrashingTheRunner covers the
// other, stricter sense of "malformed JSON reaching a live Runner": bytes
// that are not valid JSON at all. Handle rejects these identically to the
// valid-but-wrong-shaped case above, but deadLetter cannot actually record
// one as a Dead Letter — cqrs.DeadLetter.Envelope is a json.RawMessage,
// which requires its content to already be valid JSON to marshal
// successfully as part of the surrounding struct — so this exercises
// deadLetter's other settlement path: log the marshal failure accurately
// and Nak, rather than crash or hang. The Runner and this same Handler
// must still work afterward regardless.
func TestRunner_InvalidJSONSyntaxIsNakedWithoutCrashingTheRunner(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsjs.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	var invocations atomic.Int32
	handler := cqrs.NewCommandHandler("create_order", func(_ context.Context, _ testCreateOrder) error {
		invocations.Add(1)
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "invalid-json-svc",
		natsjs.WithMaxDeliver(2),
		natsjs.WithAckWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	waitForConsumer(t, nc, natsjs.CommandsStreamName, "invalid-json-svc_create_order")

	if _, err := js.Publish(ctx, handler.Subject(), []byte("this is not json at all")); err != nil {
		t.Fatalf("Publish (invalid JSON): %v", err)
	}

	// Nothing to wait for beyond "the runner keeps working": there is no
	// Dead Letter to observe for this case (see the doc comment above),
	// and the registered function itself must never run for it.
	time.Sleep(500 * time.Millisecond)
	if invocations.Load() != 0 {
		t.Errorf("handler function ran %d time(s); invalid JSON must never reach the registered function", invocations.Load())
	}

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-after-invalid-json"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitUntil(t, 5*time.Second, "the runner keeps serving its handler after an invalid-JSON message", func() bool {
		return invocations.Load() == 1
	})
}

// TestRunner_ConsumerCreationFailureForOneHandlerSurfacesFromRunAndStopsTheOthers
// is part of FIX 3's test coverage: before the fix, if one Handler's
// CreateOrUpdateConsumer failed, its runHandler goroutine returned but
// every other Handler's goroutine just kept blocking on ctx.Done() —
// Run itself would not return, and therefore not report the failure,
// until its caller's own ctx was cancelled for entirely unrelated
// reasons (e.g. ordinary shutdown), which could be arbitrarily far in
// the future. The startup failure was invisible until then.
//
// Two Command Handlers built from the very same Command type inevitably
// collide: both filter on the identical subject on the commands
// work-queue stream, which nats-server rejects as an overlapping filter
// (JSConsumerWQConsumerNotUniqueErr, error code 10100) for whichever of
// the two loses the race to register its consumer first.
func TestRunner_ConsumerCreationFailureForOneHandlerSurfacesFromRunAndStopsTheOthers(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	handlerA := cqrs.NewCommandHandler("create_order_a", func(_ context.Context, _ testCreateOrder) error { return nil })
	handlerB := cqrs.NewCommandHandler("create_order_b", func(_ context.Context, _ testCreateOrder) error { return nil })

	runner, err := natsjs.New(ctx, nc, "colliding-svc")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handlerA, handlerB); err != nil {
		t.Fatalf("Register: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(runCtx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("Run: want a non-nil error when two Handlers collide on one work-queue filter subject, got nil")
		}
		t.Logf("Run correctly reported: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not report the consumer-creation failure within the timeout; " +
			"it is presumably still blocked on the healthy handler's ctx.Done()")
	}
}

// TestRunner_MalformedEnvelopePublishedDirectlyDeadLettersImmediately is
// this fix's test for the library's own decode failures being permanent by
// construction: typedHandler.Handle now wraps cqrs.ErrUnprocessable for
// an envelope name mismatch (see handler.go), so a malformed envelope
// published straight to a Command subject — exactly like
// TestRunner_MalformedEnvelopeThatIsValidJSONIsDeadLetteredWithoutCrashingTheRunner
// above — must dead-letter on the very first attempt, not after riding out
// the ordinary nak/redeliver ladder.
//
// MaxDeliver is deliberately generous (5) and AckWait deliberately long
// relative to how quickly the Dead Letter is expected to show up: the
// ordinary ladder could not possibly reach MaxDeliver, and therefore could
// not dead-letter at all, before at least one full AckWait has elapsed. A
// Dead Letter observed well before that, with Attempts: 1, is direct proof
// the fast path — not the ladder — is what produced it.
func TestRunner_MalformedEnvelopePublishedDirectlyDeadLettersImmediately(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsjs.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	var invocations atomic.Int32
	handler := cqrs.NewCommandHandler("create_order", func(_ context.Context, _ testCreateOrder) error {
		invocations.Add(1)
		return nil
	})

	dlqCh := make(chan cqrs.DeadLetter, 1)
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if json.Unmarshal(msg.Data, &dl) == nil {
			select {
			case dlqCh <- dl:
			default:
			}
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	const ackWait = 3 * time.Second
	runner, err := natsjs.New(ctx, nc, "malformed-immediate-svc",
		natsjs.WithMaxDeliver(5),
		natsjs.WithAckWait(ackWait),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	waitForConsumer(t, nc, natsjs.CommandsStreamName, "malformed-immediate-svc_create_order")

	malformed := []byte(`{"name":"not_the_expected_message_name","payload":{}}`)
	sendTime := time.Now()
	if _, err := js.Publish(ctx, handler.Subject(), malformed); err != nil {
		t.Fatalf("Publish (malformed): %v", err)
	}

	var dl cqrs.DeadLetter
	select {
	case dl = <-dlqCh:
	case <-time.After(ackWait):
		t.Fatalf("timed out waiting for the malformed envelope to be dead-lettered within a single AckWait; " +
			"it should have been dead-lettered immediately, without waiting at all")
	}
	elapsed := time.Since(sendTime)
	if elapsed >= ackWait {
		t.Errorf("dead letter arrived after %s, at or past AckWait (%s): this indicates the ordinary nak ladder ran instead of the immediate fast path", elapsed, ackWait)
	}

	if dl.Handler != "create_order" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "create_order")
	}
	if dl.Attempts != 1 {
		t.Errorf("DeadLetter.Attempts = %d, want 1: a malformed envelope must be dead-lettered on the first attempt", dl.Attempts)
	}
	if invocations.Load() != 0 {
		t.Errorf("handler function ran %d time(s); a malformed envelope must never reach the registered function", invocations.Load())
	}

	// The runner and this same handler must still be alive afterward.
	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-after-malformed-immediate"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitUntil(t, 5*time.Second, "the runner keeps serving its handler after a malformed message", func() bool {
		return invocations.Load() == 1
	})
}
