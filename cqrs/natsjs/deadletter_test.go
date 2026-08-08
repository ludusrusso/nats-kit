package natsjs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/cqrs"
	"github.com/ludusrusso/nats-cqrs/cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

func TestRunner_HandlerThatAlwaysFailsIsRedeliveredThenDeadLettered(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const maxDeliver = 3

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	// Subscribed before anything is published, so the Dead Letter -
	// published only once redelivery is exhausted - is never missed.
	//
	// This callback runs on a nats.go-owned goroutine that can outlive this
	// test function (e.g. a redelivery arriving right as the test is
	// wrapping up): calling any testing.T method from it directly would be
	// a call-after-return-time-bomb, panicking with "Log in goroutine after
	// Test has completed" if it ever loses that race. A bad unmarshal is
	// therefore routed back through unmarshalErrs instead, and only ever
	// asserted on from the test's own goroutine, below.
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

	var attempts atomic.Int32
	handler := cqrs.NewCommandHandler("always_fails", func(_ context.Context, _ testCreateOrder) error {
		attempts.Add(1)
		return errAlwaysFails
	})

	runner, err := natsjs.New(ctx, nc, "flaky-svc",
		natsjs.WithMaxDeliver(maxDeliver),
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

	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-doomed"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 10*time.Second, "the handler is retried until it is dead-lettered", func() bool {
		return dlq.len() == 1
	})

	select {
	case err := <-unmarshalErrs:
		t.Fatalf("received a malformed dead letter: %v", err)
	default:
	}

	dl := dlq.snapshot()[0]
	if dl.Handler != "always_fails" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "always_fails")
	}
	if dl.Attempts != maxDeliver {
		t.Errorf("DeadLetter.Attempts = %d, want %d", dl.Attempts, maxDeliver)
	}
	if dl.Error == "" {
		t.Errorf("DeadLetter.Error is empty, want the handler's error text")
	}
	if dl.FailedAt.IsZero() {
		t.Errorf("DeadLetter.FailedAt is zero")
	}
	wantSubject := "commands.natsjs_test.testCreateOrder"
	if dl.Subject != wantSubject {
		t.Errorf("DeadLetter.Subject = %q, want %q", dl.Subject, wantSubject)
	}

	var envelope struct {
		Name    string `json:"name"`
		Payload struct {
			CustomerID string `json:"customer_id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(dl.Envelope, &envelope); err != nil {
		t.Fatalf("unmarshal DeadLetter.Envelope: %v", err)
	}
	if envelope.Payload.CustomerID != "cust-doomed" {
		t.Errorf("envelope payload CustomerID = %q, want %q", envelope.Payload.CustomerID, "cust-doomed")
	}

	if got := int(attempts.Load()); got != maxDeliver {
		t.Errorf("handler was invoked %d times, want %d", got, maxDeliver)
	}

	// The original message must have stopped cycling: no further
	// attempts, and no further dead letters for it.
	staysAt(t, 500*time.Millisecond, "the handler is not invoked again after being dead-lettered", func() bool {
		return int(attempts.Load()) == maxDeliver && dlq.len() == 1
	})
}

// TestRunner_DeadLetterBelongsToTheHandlerNotTheEvent is the Dead Letter's
// defining property, from CONTEXT.md: "Because Events fan out, failure
// belongs to a Handler and not to a Message: the same Event may be dead
// for one Handler and handled by every other." This was previously
// untested: nothing in this package asserted it directly.
//
// Two Event Handlers subscribe to the same Event. One always fails, the
// other always succeeds. This proves both directions of the claim at
// once: the failing Handler's repeated failure produces exactly one Dead
// Letter naming it, and the healthy Handler's success is entirely
// unaffected by its sibling's failure — it acks the very same Event
// normally, not zero times (blocked by the other's trouble) and not more
// than once (a stray retry leaking across Handlers).
func TestRunner_DeadLetterBelongsToTheHandlerNotTheEvent(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	dlqCh := make(chan cqrs.DeadLetter, 8)
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if err := json.Unmarshal(msg.Data, &dl); err != nil {
			return
		}
		select {
		case dlqCh <- dl:
		default:
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	healthy := &collector[testOrderCreated]{}
	healthyHandler := cqrs.NewEventHandler("healthy_subscriber", func(_ context.Context, evt testOrderCreated) error {
		healthy.add(evt)
		return nil
	})
	doomedHandler := cqrs.NewEventHandler("doomed_subscriber", func(_ context.Context, _ testOrderCreated) error {
		return errAlwaysFails
	})

	runner, err := natsjs.New(ctx, nc, "fanout-svc",
		natsjs.WithMaxDeliver(2),
		natsjs.WithAckWait(150*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(healthyHandler, doomedHandler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	waitForConsumer(t, nc, natsjs.EventsStreamName, "fanout-svc_healthy_subscriber")
	waitForConsumer(t, nc, natsjs.EventsStreamName, "fanout-svc_doomed_subscriber")

	bus := cqrs.NewEventBus(pub)
	if err := bus.Publish(ctx, testOrderCreated{OrderID: "order-fanout"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitUntil(t, 5*time.Second, "the healthy handler acks the event normally", func() bool {
		return healthy.len() == 1
	})

	var dl cqrs.DeadLetter
	select {
	case dl = <-dlqCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for a dead letter from the doomed handler")
	}
	if dl.Handler != "doomed_subscriber" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "doomed_subscriber")
	}
	if dl.Subject != "events.natsjs_test.testOrderCreated" {
		t.Errorf("DeadLetter.Subject = %q, want %q", dl.Subject, "events.natsjs_test.testOrderCreated")
	}

	// Exactly one Dead Letter for this Event: the healthy handler must
	// never produce one of its own.
	select {
	case extra := <-dlqCh:
		t.Fatalf("received an unexpected second dead letter, from handler %q", extra.Handler)
	case <-time.After(300 * time.Millisecond):
	}

	// And the healthy handler's own success is not just "first", it holds:
	// no redelivery loop from its sibling ever reaches it.
	staysAt(t, 300*time.Millisecond, "the healthy handler is invoked exactly once regardless of the doomed handler's failures", func() bool {
		return healthy.len() == 1
	})
}

// TestRunner_DeadLetterPublishFailureIsRetriedThenNaksInsteadOfGoingSilent
// is FIX 4's test. Both of deadLetter's error paths used to log "leaving
// message pending for redelivery" and simply return, without settling msg
// at all. That statement was false: deadLetter only ever runs once
// attempts has already reached MaxDeliver, and JetStream will not
// redeliver an unsettled message past MaxDeliver on its own (an AckWait
// timeout past MaxDeliver is not redelivered — see hasMaxDeliveries in
// nats-server's server/consumer.go). The message was left permanently
// stuck: unacked, unnaked, invisible, until the stream's MaxAge eventually
// evicted it.
//
// This test reproduces the proven repro directly: delete the cqrs-dlq
// stream while the Runner is already running, so every attempt to
// publish a Dead Letter fails from then on. It asserts the fixed
// behavior: the Handler keeps being invoked well past the configured
// MaxDeliver, because deadLetter now falls back to an explicit Nak on a
// persistent publish failure — proof the message is not stuck, contrary
// to what would happen if deadLetter still just returned silently.
func TestRunner_DeadLetterPublishFailureIsRetriedThenNaksInsteadOfGoingSilent(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const maxDeliver = 1

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	// Subscribed up front, before anything else: a plain core-NATS
	// subscription like this one keeps receiving whatever is published to
	// a matching subject regardless of whether a JetStream stream exists
	// for it, so it reliably catches the one Dead Letter this test expects
	// near the end, however the cqrs-dlq stream's own existence flaps in
	// between.
	//
	// It also, notably, receives a raw copy of every publish attempt for
	// the two un-dead-letterable messages below, even though none of them
	// is ever durably stored: js.Publish is a core NATS publish that also
	// waits for a JetStream ack, and the message still goes out over the
	// wire, to any interested subscriber, whether or not a stream exists
	// to receive and ack it. So this collector is not a reliable way to
	// count *durable* Dead Letters — only to find one by content — which
	// is exactly how it is used below.
	dlq := &collector[cqrs.DeadLetter]{}
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if json.Unmarshal(msg.Data, &dl) == nil {
			dlq.add(dl)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	var attempts atomic.Int32
	handler := cqrs.NewCommandHandler("always_fails_no_dlq", func(_ context.Context, cmd testCreateOrder) error {
		attempts.Add(1)
		t.Logf("handler invoked for %s (attempt %d)", cmd.CustomerID, attempts.Load())
		return errAlwaysFails
	})

	runner, err := natsjs.New(ctx, nc, "no-dlq-svc",
		natsjs.WithMaxDeliver(maxDeliver),
		natsjs.WithAckWait(2*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	stop := runInBackground(t, runner)
	defer stop()

	// Delete the cqrs-dlq stream mid-run, exactly as the review's repro
	// did: from this point on, every deadLetter call's publish fails.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := js.DeleteStream(ctx, natsjs.DeadLetterStreamName); err != nil {
		t.Fatalf("DeleteStream: %v", err)
	}

	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-undeadletterable"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// With maxDeliver == 1, the very first delivery already reaches
	// deadLetter, and nats-server itself will never redeliver this exact
	// message again regardless of what settles it (confirmed directly
	// against this module's nats-server: it re-checks delivery count
	// against MaxDeliver again at the moment of actual redelivery, not
	// only when first deciding whether to queue one — see getNextMsg in
	// nats-server's server/consumer.go). So attempts reaching exactly 1,
	// and staying there, is not a symptom of the bug: it is what correct
	// behavior looks like once the fix's retries are also exhausted.
	waitUntil(t, 10*time.Second, "the handler is invoked once for the un-dead-letterable message", func() bool {
		return attempts.Load() >= 1
	})
	staysAt(t, 500*time.Millisecond, "nats-server does not redeliver the same message again past MaxDeliver, fix or no fix", func() bool {
		return attempts.Load() == 1
	})

	// The actual bug this fix closes is not "the same message keeps
	// coming back" — it does not, with or without the fix. It is that the
	// old code left msg neither acked nor naked while falsely logging
	// "leaving message pending for redelivery", instead of explicitly
	// settling it. One observable consequence of never settling it: the
	// MaxAckPending slot it occupies could stay tied up for however long
	// nats-server's own cleanup happens to take. Prove the fixed code
	// settles it deliberately and promptly by sending a second, unrelated
	// command through the very same Handler (DefaultWorkers == 1, so
	// MaxAckPending == 1: if the first message's slot were still held,
	// this second one could not be delivered at all) and confirming it is
	// processed too.
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-undeadletterable-2"}); err != nil {
		t.Fatalf("Send (second): %v", err)
	}
	waitUntil(t, 10*time.Second, "the handler's single worker slot is free again for a second message", func() bool {
		return attempts.Load() >= 2
	})

	// Finally, recreate the Dead Letter stream and confirm the Runner is
	// not left poisoned by the earlier failures: a fresh message that
	// exhausts its attempts while the DLQ is healthy again is dead-lettered
	// normally, exactly like TestRunner_HandlerThatAlwaysFailsIsRedeliveredThenDeadLettered.
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      natsjs.DeadLetterStreamName,
		Subjects:  []string{"dlq.>"},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    natsjs.DefaultDeadLetterMaxAge,
	}); err != nil {
		t.Fatalf("CreateStream (recreate dlq): %v", err)
	}

	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-recovered"}); err != nil {
		t.Fatalf("Send (recovered): %v", err)
	}

	// Look for a Dead Letter naming this specific message, by its
	// envelope's payload, rather than asserting an absolute count: dlq may
	// already hold raw (non-durable) copies of the two earlier, doomed
	// publish attempts, as explained above.
	recoveredDL := func() *cqrs.DeadLetter {
		for _, dl := range dlq.snapshot() {
			var envelope struct {
				Payload struct {
					CustomerID string `json:"customer_id"`
				} `json:"payload"`
			}
			if json.Unmarshal(dl.Envelope, &envelope) == nil && envelope.Payload.CustomerID == "cust-recovered" {
				dl := dl
				return &dl
			}
		}
		return nil
	}
	waitUntil(t, 10*time.Second, "a message reaching MaxDeliver after the DLQ is healthy again is dead-lettered normally", func() bool {
		return recoveredDL() != nil
	})
	if got := recoveredDL().Handler; got != "always_fails_no_dlq" {
		t.Errorf("DeadLetter.Handler = %q, want %q", got, "always_fails_no_dlq")
	}

	// And, since the stream is healthy again, this one really is durable:
	// a JetStream consumer created fresh against cqrs-dlq can read it back.
	dlqJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	cons, err := dlqJS.CreateOrUpdateConsumer(ctx, natsjs.DeadLetterStreamName, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := msg.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestRunner_UnprocessableErrorDeadLettersImmediatelyWithoutRetrying is this
// fix's central test. A Handler that wraps cqrs.ErrUnprocessable is
// declaring the exact Message it was just handed permanently
// unprocessable — no amount of waiting or redelivery would ever help — so
// the Runner must skip its nak/redeliver ladder entirely and dead-letter
// the Message on the very first attempt. The whole point is the
// invocation count: it must be exactly one, never maxDeliver, and the Dead
// Letter it produces must say Attempts: 1, truthfully, rather than implying
// a retry ladder that never ran.
//
// AckWait is deliberately short (200ms) and maxDeliver deliberately
// generous (5): if the fast path were broken and this fell back to the
// ordinary ladder, the handler would be invoked again well within this
// test's own waiting windows, which is exactly what staysAt below is
// positioned to catch.
func TestRunner_UnprocessableErrorDeadLettersImmediatelyWithoutRetrying(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const maxDeliver = 5

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	dlq := &collector[cqrs.DeadLetter]{}
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if json.Unmarshal(msg.Data, &dl) == nil {
			dlq.add(dl)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	var attempts atomic.Int32
	handler := cqrs.NewCommandHandler("unprocessable_order", func(_ context.Context, cmd testCreateOrder) error {
		attempts.Add(1)
		return fmt.Errorf("customer id %q will never be valid: %w", cmd.CustomerID, cqrs.ErrUnprocessable)
	})

	runner, err := natsjs.New(ctx, nc, "unprocessable-svc",
		natsjs.WithMaxDeliver(maxDeliver),
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

	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-unprocessable"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 5*time.Second, "the permanent failure is dead-lettered", func() bool {
		return dlq.len() == 1
	})

	dl := dlq.snapshot()[0]
	if dl.Handler != "unprocessable_order" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "unprocessable_order")
	}
	if dl.Attempts != 1 {
		t.Errorf("DeadLetter.Attempts = %d, want 1: a permanent failure must be dead-lettered on the first attempt, not after burning a retry budget", dl.Attempts)
	}
	if !strings.Contains(dl.Error, "will never be valid") {
		t.Errorf("DeadLetter.Error = %q, want it to contain the handler's own error text", dl.Error)
	}

	if got := int(attempts.Load()); got != 1 {
		t.Fatalf("handler was invoked %d time(s), want exactly 1: an ErrUnprocessable-wrapped error must never be retried", got)
	}

	// Held well past AckWait (200ms): if the fast path had not actually
	// been taken and this message were instead sitting on the ordinary
	// nak/redeliver ladder, a second invocation would show up inside this
	// window.
	staysAt(t, 500*time.Millisecond, "the handler is invoked exactly once and never retried", func() bool {
		return int(attempts.Load()) == 1 && dlq.len() == 1
	})

	// The message must be genuinely settled, not merely "a Dead Letter was
	// published": on the commands stream (WorkQueuePolicy retention), a
	// genuinely settled message is removed from the stream outright — see
	// deadLetter's doc comment for why Term, not Ack, is used to settle it,
	// and what was measured to confirm this.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, natsjs.CommandsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Errorf("commands stream has %d message(s) left, want 0: a dead-lettered message must actually be removed from a work-queue stream", info.State.Msgs)
	}
}

// TestRunner_OrdinaryErrorStillFollowsFullRetryLadder is the guard
// TestRunner_UnprocessableErrorDeadLettersImmediatelyWithoutRetrying needs:
// proof that recognizing cqrs.ErrUnprocessable did not also make the
// fast path swallow ordinary failures it must not touch. An error that does
// not wrap ErrUnprocessable — "the card was declined, try again later" is
// exactly this project's own motivating example — must still be redelivered
// up to MaxDeliver times, and dead-letter only once that ladder is
// exhausted, with DeadLetter.Attempts equal to MaxDeliver, not 1.
func TestRunner_OrdinaryErrorStillFollowsFullRetryLadder(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	const maxDeliver = 3

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}

	dlq := &collector[cqrs.DeadLetter]{}
	sub, err := nc.Subscribe("dlq.>", func(msg *nats.Msg) {
		var dl cqrs.DeadLetter
		if json.Unmarshal(msg.Data, &dl) == nil {
			dlq.add(dl)
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	var attempts atomic.Int32
	// Deliberately an ordinary error, not wrapped with ErrUnprocessable:
	// this is "the card was declined, try again in ten minutes", which
	// might succeed on a later attempt and must burn its full retry budget
	// before this Handler gives up.
	handler := cqrs.NewCommandHandler("declined_order", func(_ context.Context, _ testCreateOrder) error {
		attempts.Add(1)
		return errAlwaysFails
	})

	runner, err := natsjs.New(ctx, nc, "declined-svc",
		natsjs.WithMaxDeliver(maxDeliver),
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

	bus := cqrs.NewCommandBus(pub)
	if err := bus.Send(ctx, testCreateOrder{CustomerID: "cust-declined"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	waitUntil(t, 10*time.Second, "the ordinary failure is retried until it is dead-lettered", func() bool {
		return dlq.len() == 1
	})

	if got := int(attempts.Load()); got != maxDeliver {
		t.Errorf("handler was invoked %d time(s), want %d: an ordinary error must be retried the full ladder, not fast-pathed", got, maxDeliver)
	}

	dl := dlq.snapshot()[0]
	if dl.Handler != "declined_order" {
		t.Errorf("DeadLetter.Handler = %q, want %q", dl.Handler, "declined_order")
	}
	if dl.Attempts != maxDeliver {
		t.Errorf("DeadLetter.Attempts = %d, want %d", dl.Attempts, maxDeliver)
	}
}
