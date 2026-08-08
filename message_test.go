package natscqrs_test

import (
	"strings"
	"testing"

	"github.com/ludusrusso/nats-cqrs"
)

func TestNewCommandHeader(t *testing.T) {
	h1 := natscqrs.NewCommandHeader()
	h2 := natscqrs.NewCommandHeader()

	if h1.ID == "" {
		t.Fatal("NewCommandHeader: ID is empty")
	}
	if !strings.HasPrefix(h1.ID, "cmd_") {
		t.Fatalf("NewCommandHeader: ID %q does not have prefix %q", h1.ID, "cmd_")
	}
	if h1.PublishedAt.IsZero() {
		t.Fatal("NewCommandHeader: PublishedAt is zero")
	}
	if h1.ID == h2.ID {
		t.Fatalf("NewCommandHeader: two calls produced the same ID %q", h1.ID)
	}
}

func TestNewEventHeader(t *testing.T) {
	h1 := natscqrs.NewEventHeader()
	h2 := natscqrs.NewEventHeader()

	if h1.ID == "" {
		t.Fatal("NewEventHeader: ID is empty")
	}
	if !strings.HasPrefix(h1.ID, "evt_") {
		t.Fatalf("NewEventHeader: ID %q does not have prefix %q", h1.ID, "evt_")
	}
	if h1.PublishedAt.IsZero() {
		t.Fatal("NewEventHeader: PublishedAt is zero")
	}
	if h1.ID == h2.ID {
		t.Fatalf("NewEventHeader: two calls produced the same ID %q", h1.ID)
	}
}

// TestSealedInterfaces confirms a value (not a pointer) of a user struct
// embedding CommandHeader/EventHeader satisfies Command/Event/Message.
func TestSealedInterfaces(t *testing.T) {
	var _ natscqrs.Message = testCreateOrder{}
	var _ natscqrs.Command = testCreateOrder{}
	var _ natscqrs.Message = testOrderCreated{}
	var _ natscqrs.Event = testOrderCreated{}
}

// TestSealedInterfaces_CommandAndEventAreDisjoint asserts the property that
// actually makes Command and Event sealed and separate: it is not enough
// that a Command satisfies Command and an Event satisfies Event (that's
// TestSealedInterfaces above) — a Command embedding CommandHeader must NOT
// also satisfy Event, and an Event embedding EventHeader must NOT also
// satisfy Command. Publishing an Event on the command bus, or a Command on
// the event bus, is a compile error at the call site (CommandBus.Send takes
// ...Command, EventBus.Publish takes ...Event); this test is the runtime
// shadow of that same guarantee, checked through a type assertion on a
// value held as the common Message interface, exactly how CommandBus.Send
// and EventBus.Publish (and Marshal) receive their argument internally.
func TestSealedInterfaces_CommandAndEventAreDisjoint(t *testing.T) {
	var cmdAsMessage natscqrs.Message = testCreateOrder{}
	if _, ok := cmdAsMessage.(natscqrs.Event); ok {
		t.Fatal("testCreateOrder (embeds CommandHeader) must not satisfy Event, but it does")
	}

	var evtAsMessage natscqrs.Message = testOrderCreated{}
	if _, ok := evtAsMessage.(natscqrs.Command); ok {
		t.Fatal("testOrderCreated (embeds EventHeader) must not satisfy Command, but it does")
	}
}
