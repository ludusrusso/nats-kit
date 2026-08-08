package natscqrs_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ludusrusso/nats-cqrs"
)

func TestMarshal_CommandSubjectAndID(t *testing.T) {
	original := testCreateOrder{CustomerID: "cust-1"}

	rec, err := natscqrs.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	if want := "commands.natscqrs_test.testCreateOrder"; rec.Subject != want {
		t.Fatalf("Marshal: Subject = %q, want %q", rec.Subject, want)
	}
	if rec.ID == "" {
		t.Fatal("Marshal: Record.ID is empty")
	}

	// The Record's ID must match the id embedded in the envelope's payload
	// header, and the original value passed in must be untouched.
	var env struct {
		Name    string `json:"name"`
		Payload struct {
			Header struct {
				ID string `json:"id"`
			} `json:"header"`
			CustomerID string `json:"customer_id"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(rec.Data, &env); err != nil {
		t.Fatalf("json.Unmarshal(rec.Data): unexpected error: %v", err)
	}
	if env.Payload.Header.ID != rec.ID {
		t.Fatalf("envelope payload header id = %q, want it to match Record.ID %q", env.Payload.Header.ID, rec.ID)
	}
	if env.Payload.CustomerID != "cust-1" {
		t.Fatalf("envelope payload customer_id = %q, want %q", env.Payload.CustomerID, "cust-1")
	}
	if original.ID != "" {
		t.Fatalf("Marshal mutated the caller's original value: ID = %q, want empty", original.ID)
	}
}

func TestMarshal_EventSubject(t *testing.T) {
	rec, err := natscqrs.Marshal(testOrderCreated{OrderID: "order-1"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}
	if want := "events.natscqrs_test.testOrderCreated"; rec.Subject != want {
		t.Fatalf("Marshal: Subject = %q, want %q", rec.Subject, want)
	}
}

// TestRoundTrip_MessageToRecordToHandler covers Message -> Record -> Handler
// end to end: the value a Handler's function receives must match what was
// sent, including the hydrated header ID.
func TestRoundTrip_MessageToRecordToHandler(t *testing.T) {
	sent := testCreateOrder{CustomerID: "cust-42"}

	rec, err := natscqrs.Marshal(sent)
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	var received testCreateOrder
	var gotCtx context.Context
	handler := natscqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		gotCtx = ctx
		received = cmd
		return nil
	})

	if err := handler.Validate(); err != nil {
		t.Fatalf("Handler.Validate: unexpected error: %v", err)
	}

	ctx := context.Background()
	if err := handler.Handle(ctx, rec.Data); err != nil {
		t.Fatalf("Handler.Handle: unexpected error: %v", err)
	}

	if gotCtx != ctx {
		t.Fatal("Handler.Handle: fn was not called with the given context")
	}
	if received.CustomerID != sent.CustomerID {
		t.Fatalf("received.CustomerID = %q, want %q", received.CustomerID, sent.CustomerID)
	}
	if received.ID != rec.ID {
		t.Fatalf("received.ID = %q, want %q (the Record's ID)", received.ID, rec.ID)
	}
	if received.ID == "" {
		t.Fatal("received.ID is empty")
	}
	if received.PublishedAt.IsZero() {
		t.Fatal("received.PublishedAt is zero")
	}
}

// TestHandle_EnvelopeNameMismatch confirms Handle rejects an envelope built
// for a different Message type.
func TestHandle_EnvelopeNameMismatch(t *testing.T) {
	otherRec, err := natscqrs.Marshal(testOrderCreated{OrderID: "order-1"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	handler := natscqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		return nil
	})

	if err := handler.Handle(context.Background(), otherRec.Data); err == nil {
		t.Fatal("Handler.Handle: want error for mismatched envelope name, got nil")
	}
}

func TestMarshal_RejectsMissingHeaderTag(t *testing.T) {
	type badCommand struct {
		natscqrs.CommandHeader
		Value string `json:"value"`
	}

	if _, err := natscqrs.Marshal(badCommand{}); err == nil {
		t.Fatal("Marshal: want error for a message with no json:\"header\" tag, got nil")
	}
}
