package outbox_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/ludusrusso/nats-kit/cqrs"
	"github.com/ludusrusso/nats-kit/cqrs/outbox"
)

func TestPublish_LandsInOutbox_NotYetAtSink(t *testing.T) {
	mem := outbox.NewMemory()
	events := cqrs.NewEventBus(mem)

	err := events.Publish(context.Background(),
		testOrderCreated{OrderID: "1"},
		testOrderCreated{OrderID: "2"},
		testOrderCreated{OrderID: "3"},
	)
	if err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	if got := mem.Pending(); got != 3 {
		t.Fatalf("mem.Pending() = %d, want 3", got)
	}

	sink := newRecordingSink(nil)
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink.Publish was called %d times before any forwarding, want 0", got)
	}
}

func TestOnce_DrainsBatchToSink_PreservingBatch(t *testing.T) {
	mem := outbox.NewMemory()
	events := cqrs.NewEventBus(mem)

	if err := events.Publish(context.Background(),
		testOrderCreated{OrderID: "1"},
		testOrderCreated{OrderID: "2"},
	); err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	sink := newRecordingSink(nil)
	fwd := outbox.NewForwarder(mem, sink)

	sent, err := fwd.Once(context.Background())
	if err != nil {
		t.Fatalf("Once: unexpected error: %v", err)
	}
	if sent != 2 {
		t.Fatalf("Once: sent = %d, want 2", sent)
	}
	if got := mem.Pending(); got != 0 {
		t.Fatalf("mem.Pending() after Once = %d, want 0", got)
	}

	// The whole batch must arrive at the sink in one call, exactly as it
	// was written, for the same reason CommandBus/EventBus hand a whole
	// batch to Sink.Publish in one call: a batch stays a batch.
	if got := sink.callCount(); got != 1 {
		t.Fatalf("sink.Publish was called %d times, want 1 (one batch, one call)", got)
	}
	if got := len(sink.batchAt(0)); got != 2 {
		t.Fatalf("delivered batch has %d records, want 2", got)
	}
}

func TestOnce_SinkFailure_NothingIsLost_LaterPassDelivers(t *testing.T) {
	mem := outbox.NewMemory()
	events := cqrs.NewEventBus(mem)

	if err := events.Publish(context.Background(), testOrderCreated{OrderID: "1"}); err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	boom := errors.New("boom: NATS unreachable")
	sink := newRecordingSink(boom)
	fwd := outbox.NewForwarder(mem, sink)

	sent, err := fwd.Once(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Once: error = %v, want %v", err, boom)
	}
	if sent != 0 {
		t.Fatalf("Once: sent = %d, want 0 when the sink fails", sent)
	}

	// Nothing is lost: the Record must still be pending, ready for retry.
	if got := mem.Pending(); got != 1 {
		t.Fatalf("mem.Pending() after a failed pass = %d, want 1: the record must not be lost", got)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink recorded %d successful batches, want 0", got)
	}

	// The sink recovers; the next pass delivers the very same Record.
	sink.setErr(nil)
	sent, err = fwd.Once(context.Background())
	if err != nil {
		t.Fatalf("second Once: unexpected error: %v", err)
	}
	if sent != 1 {
		t.Fatalf("second Once: sent = %d, want 1", sent)
	}
	if got := mem.Pending(); got != 0 {
		t.Fatalf("mem.Pending() after successful delivery = %d, want 0", got)
	}
	ids := sink.allIDs()
	if len(ids) != 1 || ids[0] == "" {
		t.Fatalf("delivered record IDs = %v, want exactly one non-empty ID", ids)
	}
}

func TestReadForSend_PanicInSend_LeavesRecordPending(t *testing.T) {
	mem := outbox.NewMemory()
	events := cqrs.NewEventBus(mem)

	if err := events.Publish(context.Background(), testOrderCreated{OrderID: "1"}); err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	// Drive ReadForSend directly with a send callback that panics, and
	// recover from it here, exactly as a caller unwinding out of a
	// crashed send would. The point of this test is what happens to the
	// Record afterward: it must come back to pending, not stay held
	// forever just because send never returned normally.
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("ReadForSend: send did not panic as expected")
			}
		}()
		_, _ = mem.ReadForSend(context.Background(), 10, func(context.Context, []cqrs.Record) error {
			panic("boom: send panicked")
		})
	}()

	if got := mem.Pending(); got != 1 {
		t.Fatalf("mem.Pending() after a panicking send = %d, want 1: the record must not be left stuck held", got)
	}

	// Pending() reporting 1 is not enough on its own: prove the Record is
	// genuinely usable again by having a later pass actually claim and
	// deliver it, exactly as the failed-send retry path does.
	sink := newRecordingSink(nil)
	fwd := outbox.NewForwarder(mem, sink)
	sent, err := fwd.Once(context.Background())
	if err != nil {
		t.Fatalf("Once after recovering a panicking send: unexpected error: %v", err)
	}
	if sent != 1 {
		t.Fatalf("Once after recovering a panicking send: sent = %d, want 1", sent)
	}
	if got := mem.Pending(); got != 0 {
		t.Fatalf("mem.Pending() after the retried pass = %d, want 0", got)
	}
}

func TestOnce_ConcurrentCalls_NeverDeliverSameRecordTwice(t *testing.T) {
	mem := outbox.NewMemory()
	events := cqrs.NewEventBus(mem)

	const total = 200
	msgs := make([]cqrs.Event, 0, total)
	for i := 0; i < total; i++ {
		msgs = append(msgs, testOrderCreated{OrderID: strconv.Itoa(i)})
	}
	if err := events.Publish(context.Background(), msgs...); err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	sink := newRecordingSink(nil)
	fwd := outbox.NewForwarder(mem, sink, outbox.WithBatchSize(7))

	const workers = 10
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			for {
				sent, err := fwd.Once(context.Background())
				if err != nil {
					errCh <- err
					return
				}
				if sent == 0 {
					errCh <- nil
					return
				}
			}
		}()
	}
	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Once: unexpected error from a concurrent worker: %v", err)
		}
	}

	if got := mem.Pending(); got != 0 {
		t.Fatalf("mem.Pending() after concurrent drain = %d, want 0", got)
	}

	ids := sink.allIDs()
	if len(ids) != total {
		t.Fatalf("sink delivered %d records total, want %d", len(ids), total)
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" {
			t.Fatalf("a delivered record had an empty ID")
		}
		if seen[id] {
			t.Fatalf("record with ID %q was delivered more than once", id)
		}
		seen[id] = true
	}
}
