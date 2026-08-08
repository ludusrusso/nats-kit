package outbox_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	natscqrs "github.com/ludusrusso/nats-cqrs"
	"github.com/ludusrusso/nats-cqrs/outbox"
)

// publishN publishes n distinct Events into bus, failing the test on error.
func publishN(t *testing.T, events *natscqrs.EventBus, n int) {
	t.Helper()
	msgs := make([]natscqrs.Event, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, testOrderCreated{OrderID: strconv.Itoa(i)})
	}
	if err := events.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}
}

func TestRun_DrainsBacklog_AndStopsPromptlyOnCancel(t *testing.T) {
	mem := outbox.NewMemory()
	publishN(t, natscqrs.NewEventBus(mem), 250)

	sink := newRecordingSink(nil)
	fwd := outbox.NewForwarder(mem, sink,
		outbox.WithBatchSize(50),
		outbox.WithPollInterval(10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fwd.Run(ctx) }()

	waitUntilSinkReceived(t, sink, 250, 2*time.Second)

	if got := sink.recordCount(); got != 250 {
		t.Fatalf("sink delivered %d records before cancellation, want 250", got)
	}

	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run did not return within 1s of cancellation")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("Run took %v to return after cancellation, want a prompt return", elapsed)
	}
}

func TestRun_FullBatchTriggersImmediateNextPass(t *testing.T) {
	mem := outbox.NewMemory()

	const batch = 10
	const total = 25 // two full batches (10, 10) plus a partial one (5)
	publishN(t, natscqrs.NewEventBus(mem), total)

	sink := newRecordingSink(nil)
	// The poll interval is deliberately long: if Run waited it out between
	// the two full-batch passes instead of repolling immediately, the
	// backlog could not possibly drain within the short deadline below.
	fwd := outbox.NewForwarder(mem, sink,
		outbox.WithBatchSize(batch),
		outbox.WithPollInterval(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fwd.Run(ctx) }()

	waitUntilSinkReceived(t, sink, total, 300*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Run did not return within 1s of cancellation")
	}

	if got := sink.recordCount(); got != total {
		t.Fatalf("sink delivered %d records, want %d", got, total)
	}
}

func TestRun_LogsAndRetriesOnError_NeverTerminates(t *testing.T) {
	mem := outbox.NewMemory()
	publishN(t, natscqrs.NewEventBus(mem), 1)

	boom := errors.New("boom")
	sink := newRecordingSink(boom)
	fwd := outbox.NewForwarder(mem, sink, outbox.WithPollInterval(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fwd.Run(ctx) }()

	// Let it fail a few times before letting it succeed: wait for the
	// observed failure count to reach a small threshold, via a polling
	// loop with a timeout, rather than sleeping a fixed duration and
	// hoping the poll interval produced enough passes in that time.
	const wantFailures = 3
	waitUntilFailCount(t, sink, wantFailures, time.Second)
	sink.setErr(nil)

	waitUntilSinkReceived(t, sink, 1, time.Second)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run did not return within 1s of cancellation")
	}

	if got := sink.recordCount(); got != 1 {
		t.Fatalf("sink delivered %d records, want 1", got)
	}
}

// waitUntilSinkReceived polls sink until it has recorded at least want
// Records, or deadline elapses.
//
// This waits on the sink's own observed count rather than on
// mem.Pending() reaching 0, because Memory.ReadForSend marks entries held
// — making them invisible to Pending() — before calling send: the final
// batch can already have vanished from Pending() while it is still inside
// send, not yet delivered to sink. Waiting on Pending() would therefore
// let the test read sink.recordCount() before the last batch actually
// landed — a real race, even though today's in-memory recordingSink
// happens not to expose it. Asserting on the sink's count is asserting on
// the property the test actually cares about.
func waitUntilSinkReceived(t *testing.T, sink *recordingSink, want int, deadline time.Duration) {
	t.Helper()
	giveUp := time.After(deadline)
	for {
		if sink.recordCount() >= want {
			return
		}
		select {
		case <-giveUp:
			t.Fatalf("sink received %d records within %v, want %d", sink.recordCount(), deadline, want)
		case <-time.After(time.Millisecond):
		}
	}
}

// waitUntilFailCount polls sink until it has recorded at least want failed
// Publish calls, or deadline elapses. It exists so tests that need "a few
// failed passes to have happened" can wait for that fact directly, instead
// of sleeping a fixed duration and hoping the poll interval produced
// enough passes in that time.
func waitUntilFailCount(t *testing.T, sink *recordingSink, want int, deadline time.Duration) {
	t.Helper()
	giveUp := time.After(deadline)
	for {
		if sink.failCount() >= want {
			return
		}
		select {
		case <-giveUp:
			t.Fatalf("sink recorded %d failed passes within %v, want at least %d", sink.failCount(), deadline, want)
		case <-time.After(time.Millisecond):
		}
	}
}
