package cqrs_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ludusrusso/nats-kit/cqrs"
)

// testOrder is a domain object that gets its Event buffer from the mixin, the
// only way an Aggregate is ever used.
type testOrder struct {
	cqrs.Aggregate
}

// orderCreated builds an Event whose identity is already set, so the ID of the
// Record it becomes is the one a test asserts on: Marshal fills in a missing
// ID, never an existing one.
func orderCreated(orderID string) testOrderCreated {
	return testOrderCreated{
		EventHeader: cqrs.EventHeader{ID: "evt-" + orderID},
		OrderID:     orderID,
	}
}

func orderIDs(t *testing.T, events []cqrs.Event) []string {
	t.Helper()
	ids := make([]string, len(events))
	for i, e := range events {
		evt, ok := e.(testOrderCreated)
		if !ok {
			t.Fatalf("pulled event %d has type %T, want testOrderCreated", i, e)
		}
		ids[i] = evt.OrderID
	}
	return ids
}

func recordIDs(records []cqrs.Record) []string {
	ids := make([]string, len(records))
	for i, r := range records {
		ids[i] = r.ID
	}
	return ids
}

func TestAggregate_FreshAggregate_HasNothingToPull(t *testing.T) {
	var order testOrder

	if got := order.PullEvents(); len(got) != 0 {
		t.Fatalf("PullEvents on a fresh aggregate returned %d events, want 0", len(got))
	}
}

func TestAggregate_PullEvents_ReturnsRecordingOrder(t *testing.T) {
	var order testOrder
	order.Record(orderCreated("1"))
	order.Record(orderCreated("2"))

	if got, want := orderIDs(t, order.PullEvents()), []string{"1", "2"}; !slices.Equal(got, want) {
		t.Fatalf("PullEvents returned %v, want %v", got, want)
	}
}

func TestAggregate_PullEvents_SecondPullIsEmpty(t *testing.T) {
	var order testOrder
	order.Record(orderCreated("1"))

	if got := order.PullEvents(); len(got) != 1 {
		t.Fatalf("first PullEvents returned %d events, want 1", len(got))
	}
	if got := order.PullEvents(); len(got) != 0 {
		t.Fatalf("second PullEvents returned %d events, want 0 (a fact is pulled once)", len(got))
	}
}

func TestAggregate_Record_AfterAPullStartsANewBatch(t *testing.T) {
	var order testOrder
	order.Record(orderCreated("1"))
	order.PullEvents()

	order.Record(orderCreated("2"))

	if got, want := orderIDs(t, order.PullEvents()), []string{"2"}; !slices.Equal(got, want) {
		t.Fatalf("PullEvents returned %v, want %v", got, want)
	}
}

func TestDrain_PublishesRecordingOrder_AndEmptiesTheAggregate(t *testing.T) {
	var order testOrder
	order.Record(orderCreated("1"))
	order.Record(orderCreated("2"))
	sink := &fakeSink{}

	if err := cqrs.Drain(context.Background(), sink, &order); err != nil {
		t.Fatalf("Drain: unexpected error: %v", err)
	}

	if got := sink.callCount(); got != 1 {
		t.Fatalf("sink.Publish was called %d times, want 1 (one batch, one call)", got)
	}
	if got, want := recordIDs(sink.batches[0]), []string{"evt-1", "evt-2"}; !slices.Equal(got, want) {
		t.Fatalf("published record IDs = %v, want %v", got, want)
	}
	if got := order.PullEvents(); len(got) != 0 {
		t.Fatalf("aggregate still holds %d events after Drain, want 0", len(got))
	}
}

func TestDrain_FreshAggregate_DoesNotCallSink(t *testing.T) {
	var order testOrder
	sink := &fakeSink{err: errors.New("must not be called")}

	if err := cqrs.Drain(context.Background(), sink, &order); err != nil {
		t.Fatalf("Drain of a fresh aggregate: unexpected error: %v", err)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink.Publish was called %d times for an empty aggregate, want 0", got)
	}
}

func TestDrain_PropagatesSinkError(t *testing.T) {
	wantErr := errors.New("outbox unavailable")
	var order testOrder
	order.Record(orderCreated("1"))
	sink := &fakeSink{err: wantErr}

	err := cqrs.Drain(context.Background(), sink, &order)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Drain: error = %v, want %v", err, wantErr)
	}
}
