package cqrs_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ludusrusso/nats-cqrs/cqrs"
)

// orderService's OnOrderCreated method is used to prove that
// NewEventHandler infers its type parameter from a method value, with no
// explicit type argument at the call site.
type orderService struct {
	received testOrderCreated
	called   bool
}

func (s *orderService) OnOrderCreated(ctx context.Context, e testOrderCreated) error {
	s.received = e
	s.called = true
	return nil
}

func TestNewEventHandler_InfersTypeFromMethodValue(t *testing.T) {
	svc := &orderService{}

	// No explicit type argument: T is inferred as testOrderCreated from
	// svc.OnOrderCreated's signature.
	handler := cqrs.NewEventHandler("on_order_created", svc.OnOrderCreated)

	if err := handler.Validate(); err != nil {
		t.Fatalf("Validate: unexpected error: %v", err)
	}
	if got := handler.Name(); got != "on_order_created" {
		t.Fatalf("Name() = %q, want %q", got, "on_order_created")
	}
	if got := handler.Kind(); got != cqrs.KindEvent {
		t.Fatalf("Kind() = %q, want %q", got, cqrs.KindEvent)
	}
	if want := "events.cqrs_test.testOrderCreated"; handler.Subject() != want {
		t.Fatalf("Subject() = %q, want %q", handler.Subject(), want)
	}

	rec, err := cqrs.Marshal(testOrderCreated{OrderID: "order-9"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}
	if err := handler.Handle(context.Background(), rec.Data); err != nil {
		t.Fatalf("Handle: unexpected error: %v", err)
	}
	if !svc.called {
		t.Fatal("Handle: the registered method value was never invoked")
	}
	if svc.received.OrderID != "order-9" {
		t.Fatalf("received.OrderID = %q, want %q", svc.received.OrderID, "order-9")
	}
}

func TestNewCommandHandler_InfersTypeFromClosure(t *testing.T) {
	var received testCreateOrder
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		received = cmd
		return nil
	})

	if got := handler.Kind(); got != cqrs.KindCommand {
		t.Fatalf("Kind() = %q, want %q", got, cqrs.KindCommand)
	}
	if want := "commands.cqrs_test.testCreateOrder"; handler.Subject() != want {
		t.Fatalf("Subject() = %q, want %q", handler.Subject(), want)
	}

	rec, err := cqrs.Marshal(testCreateOrder{CustomerID: "cust-7"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}
	if err := handler.Handle(context.Background(), rec.Data); err != nil {
		t.Fatalf("Handle: unexpected error: %v", err)
	}
	if received.CustomerID != "cust-7" {
		t.Fatalf("received.CustomerID = %q, want %q", received.CustomerID, "cust-7")
	}
}

func TestHandler_Validate_RejectsEmptyName(t *testing.T) {
	handler := cqrs.NewCommandHandler("", func(ctx context.Context, cmd testCreateOrder) error {
		return nil
	})
	if err := handler.Validate(); err == nil {
		t.Fatal("Validate: want error for an empty handler name, got nil")
	}
}

func TestHandler_Validate_RejectsBadMessageType(t *testing.T) {
	type badCommand struct {
		cqrs.CommandHeader
		Value string `json:"value"`
	}

	handler := cqrs.NewCommandHandler("bad", func(ctx context.Context, cmd badCommand) error {
		return nil
	})
	if err := handler.Validate(); err == nil {
		t.Fatal("Validate: want error for a message with no json:\"header\" tag, got nil")
	}
}

// TestHandler_Validate_RejectsNilFn confirms Validate rejects a nil fn
// instead of letting it through to panic later, inside Handle, on the first
// message that arrives.
func TestHandler_Validate_RejectsNilFn(t *testing.T) {
	handler := cqrs.NewCommandHandler[testCreateOrder]("charge_order", nil)
	if err := handler.Validate(); err == nil {
		t.Fatal("Validate: want error for a nil fn, got nil")
	}
}

// TestHandle_RejectsNullPayload confirms Handle rejects an envelope whose
// payload is the JSON literal null. json.Unmarshal([]byte("null"), &v) is a
// documented no-op — it returns nil and leaves v untouched — so without this
// check, a null payload would silently invoke fn with a zero-valued
// Message: empty ID, zero PublishedAt, zero domain fields, no error.
func TestHandle_RejectsNullPayload(t *testing.T) {
	called := false
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		called = true
		return nil
	})

	env := []byte(`{"name":"cqrs_test.testCreateOrder","payload":null}`)
	if err := handler.Handle(context.Background(), env); err == nil {
		t.Fatal("Handle: want error for a literal-null payload, got nil")
	}
	if called {
		t.Fatal("Handle: fn must not be invoked for a null payload")
	}
}

// TestHandle_RejectsAbsentPayload confirms Handle rejects an envelope with
// no "payload" field at all, for the same reason as
// TestHandle_RejectsNullPayload: an absent field unmarshals to the same
// untouched zero-valued Message.
func TestHandle_RejectsAbsentPayload(t *testing.T) {
	called := false
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		called = true
		return nil
	})

	env := []byte(`{"name":"cqrs_test.testCreateOrder"}`)
	if err := handler.Handle(context.Background(), env); err == nil {
		t.Fatal("Handle: want error for an absent payload, got nil")
	}
	if called {
		t.Fatal("Handle: fn must not be invoked for an absent payload")
	}
}

// TestHandle_DecodeFailuresWrapErrUnprocessable confirms every failure
// Handle can produce on its own — before the registered function is ever
// invoked — wraps cqrs.ErrUnprocessable: a corrupt envelope, a Message
// Name mismatch, and an absent or null payload. These are permanent by
// construction: no amount of retrying fixes a payload that will never
// parse, so a transport must be able to recognize them via errors.Is and
// skip its retry ladder.
func TestHandle_DecodeFailuresWrapErrUnprocessable(t *testing.T) {
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		t.Fatal("Handle: fn must not be invoked for any of these malformed envelopes")
		return nil
	})

	cases := map[string][]byte{
		"invalid envelope JSON": []byte(`not json at all`),
		"name mismatch":         []byte(`{"name":"not_the_expected_name","payload":{}}`),
		"null payload":          []byte(`{"name":"cqrs_test.testCreateOrder","payload":null}`),
		"absent payload":        []byte(`{"name":"cqrs_test.testCreateOrder"}`),
		"payload does not unmarshal into the message type": []byte(`{"name":"cqrs_test.testCreateOrder","payload":"not an object"}`),
	}

	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			err := handler.Handle(context.Background(), env)
			if err == nil {
				t.Fatal("Handle: want a non-nil error, got nil")
			}
			if !errors.Is(err, cqrs.ErrUnprocessable) {
				t.Fatalf("Handle: error = %v, want it to wrap cqrs.ErrUnprocessable", err)
			}
		})
	}
}

// TestHandle_HandlerFunctionErrorIsNotWrappedByHandle confirms Handle does
// not itself inject ErrUnprocessable into whatever the registered function
// returns: wrapping ErrUnprocessable (or not) for the function's own
// returned error is entirely that function's decision, not Handle's.
func TestHandle_HandlerFunctionErrorIsNotWrappedByHandle(t *testing.T) {
	ordinary := errors.New("card declined, try again later")
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		return ordinary
	})

	rec, err := cqrs.Marshal(testCreateOrder{CustomerID: "cust-1"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	got := handler.Handle(context.Background(), rec.Data)
	if !errors.Is(got, ordinary) {
		t.Fatalf("Handle: error = %v, want it to wrap the handler function's own error", got)
	}
	if errors.Is(got, cqrs.ErrUnprocessable) {
		t.Fatalf("Handle: error = %v, want it to NOT wrap ErrUnprocessable: this function never wrapped it itself", got)
	}
}

// TestHandle_PermanentFailureFromHandlerFunctionIsRecognized confirms the
// other direction: when the registered function itself wraps
// ErrUnprocessable, that survives Handle unchanged and is visible to a
// caller via errors.Is — exactly what a transport (see the natsjs package)
// relies on to skip its retry ladder.
func TestHandle_PermanentFailureFromHandlerFunctionIsRecognized(t *testing.T) {
	handler := cqrs.NewCommandHandler("charge_order", func(ctx context.Context, cmd testCreateOrder) error {
		return fmt.Errorf("malformed customer id %q: %w", cmd.CustomerID, cqrs.ErrUnprocessable)
	})

	rec, err := cqrs.Marshal(testCreateOrder{CustomerID: "not-valid"})
	if err != nil {
		t.Fatalf("Marshal: unexpected error: %v", err)
	}

	got := handler.Handle(context.Background(), rec.Data)
	if !errors.Is(got, cqrs.ErrUnprocessable) {
		t.Fatalf("Handle: error = %v, want it to wrap cqrs.ErrUnprocessable", got)
	}
}
