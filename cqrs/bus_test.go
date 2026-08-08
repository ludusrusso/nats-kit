package cqrs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ludusrusso/nats-cqrs/cqrs"
)

func TestCommandBus_Send_BatchesInOneSinkCall(t *testing.T) {
	sink := &fakeSink{}
	bus := cqrs.NewCommandBus(sink)

	err := bus.Send(context.Background(),
		testCreateOrder{CustomerID: "a"},
		testCreateOrder{CustomerID: "b"},
	)
	if err != nil {
		t.Fatalf("Send: unexpected error: %v", err)
	}

	if got := sink.callCount(); got != 1 {
		t.Fatalf("sink.Publish was called %d times, want 1 (one batch, one call)", got)
	}
	if got := len(sink.batches[0]); got != 2 {
		t.Fatalf("batch has %d records, want 2", got)
	}
}

func TestCommandBus_Send_NoCommands_DoesNotCallSink(t *testing.T) {
	sink := &fakeSink{}
	bus := cqrs.NewCommandBus(sink)

	if err := bus.Send(context.Background()); err != nil {
		t.Fatalf("Send with no commands: unexpected error: %v", err)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink.Publish was called %d times for an empty batch, want 0", got)
	}
}

func TestCommandBus_Send_PropagatesSinkError(t *testing.T) {
	wantErr := errors.New("boom")
	sink := &fakeSink{err: wantErr}
	bus := cqrs.NewCommandBus(sink)

	err := bus.Send(context.Background(), testCreateOrder{CustomerID: "a"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Send: error = %v, want %v", err, wantErr)
	}
}

func TestEventBus_Publish_BatchesInOneSinkCall(t *testing.T) {
	sink := &fakeSink{}
	bus := cqrs.NewEventBus(sink)

	err := bus.Publish(context.Background(),
		testOrderCreated{OrderID: "1"},
		testOrderCreated{OrderID: "2"},
		testOrderCreated{OrderID: "3"},
	)
	if err != nil {
		t.Fatalf("Publish: unexpected error: %v", err)
	}

	if got := sink.callCount(); got != 1 {
		t.Fatalf("sink.Publish was called %d times, want 1 (one batch, one call)", got)
	}
	if got := len(sink.batches[0]); got != 3 {
		t.Fatalf("batch has %d records, want 3", got)
	}
}

func TestEventBus_Publish_NoEvents_DoesNotCallSink(t *testing.T) {
	sink := &fakeSink{}
	bus := cqrs.NewEventBus(sink)

	if err := bus.Publish(context.Background()); err != nil {
		t.Fatalf("Publish with no events: unexpected error: %v", err)
	}
	if got := sink.callCount(); got != 0 {
		t.Fatalf("sink.Publish was called %d times for an empty batch, want 0", got)
	}
}
