package natscqrs_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ludusrusso/nats-cqrs"
)

func TestDeadLetter_JSONRoundTrip(t *testing.T) {
	original := natscqrs.DeadLetter{
		Subject:  "commands.natscqrs_test.testCreateOrder",
		Handler:  "charge_order",
		Error:    "card declined",
		Attempts: 5,
		FailedAt: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		Envelope: json.RawMessage(`{"name":"natscqrs_test.testCreateOrder","payload":{}}`),
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal: unexpected error: %v", err)
	}

	var got natscqrs.DeadLetter
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: unexpected error: %v", err)
	}

	if got.Subject != original.Subject || got.Handler != original.Handler ||
		got.Error != original.Error || got.Attempts != original.Attempts {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, original)
	}
	if !got.FailedAt.Equal(original.FailedAt) {
		t.Fatalf("FailedAt = %v, want %v", got.FailedAt, original.FailedAt)
	}
	if string(got.Envelope) != string(original.Envelope) {
		t.Fatalf("Envelope = %s, want %s", got.Envelope, original.Envelope)
	}
}
