package natscqrs

import "time"

// Message is the root concept of this library: a Go struct — never any —
// that can travel over the bus. Message is sealed: msg is unexported, so
// the only way to satisfy Message from outside this package is to embed
// CommandHeader or EventHeader, which is exactly how a struct becomes a
// Message. Every Message carries a Header and is either a Command or an
// Event; there is no third kind, and no Message is both.
type Message interface {
	msg()
}

// Command is a Message expressing an intent: something that should happen.
// A Command is addressed to exactly one logical handler — two replicas of
// the same service compete for a Command, and only one of them handles it.
//
// Command is sealed the same way Message is: embed CommandHeader to
// implement it.
type Command interface {
	Message
	cmd()
}

// Event is a Message stating a fact: something that already happened. An
// Event is addressed to anyone interested, and each interested Handler
// receives its own copy, independently of the others. An Event that has
// been published is history; a Handler that comes into existence later is
// not owed the past.
//
// Event is sealed the same way Message is: embed EventHeader to implement
// it.
type Event interface {
	Message
	evt()
}

// CommandHeader is what every Command carries besides its own subject
// matter: an identity (ID), a moment of publication (PublishedAt), and a
// place for cross-cutting concerns that belong to the Command's journey
// rather than to its meaning — tracing, correlation — kept in Metadata.
//
// Embed it exactly like this, with an explicit json:"header" tag. The tag is
// load-bearing: without it, Go promotes ID/PublishedAt/Metadata inline into
// the message's own JSON, and a domain field whose json tag happens to
// collide with "id", "published_at" or "metadata" silently destroys the
// message's identity with no error from encoding/json. See Marshal's doc
// comment for how this library catches that mistake.
//
//	type CreateOrder struct {
//		natscqrs.CommandHeader `json:"header"`
//		CustomerID string `json:"customer_id"`
//	}
//
// Both marker methods use a value receiver, so a value of CreateOrder (not
// a pointer to it) satisfies Command: this library uses value semantics for
// Messages throughout.
//
// Metadata is where tracing/correlation data travels, and this package
// deliberately does not import OpenTelemetry to read or write it — the root
// package has zero external dependencies, by design. A caller that does
// depend on OpenTelemetry doesn't need any help from this library either:
// map[string]string already has the exact method set
// propagation.TextMapCarrier asks for, once addressed through
// propagation.MapCarrier (which is literally `type MapCarrier
// map[string]string` in the otel package), so injection and extraction are
// two lines at the call site:
//
//	header.Metadata = map[string]string{}
//	propagator.Inject(ctx, propagation.MapCarrier(header.Metadata)) // publish side
//
//	ctx = propagator.Extract(ctx, propagation.MapCarrier(cmd.Metadata)) // handler side
type CommandHeader struct {
	ID          string            `json:"id"`
	PublishedAt time.Time         `json:"published_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (CommandHeader) msg() {}
func (CommandHeader) cmd() {}

// EventHeader is what every Event carries besides its own subject matter.
// Its fields mean exactly what CommandHeader's do; see CommandHeader's doc
// comment for the json:"header" tag requirement, which applies identically
// here.
//
//	type OrderCreated struct {
//		natscqrs.EventHeader `json:"header"`
//		OrderID string `json:"order_id"`
//	}
type EventHeader struct {
	ID          string            `json:"id"`
	PublishedAt time.Time         `json:"published_at"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (EventHeader) msg() {}
func (EventHeader) evt() {}

// NewCommandHeader returns a CommandHeader with a freshly generated ID and
// PublishedAt set to the current time (UTC), for callers that need the
// Command's identity established before it is ever handed to CommandBus.Send
// — e.g. to log it, to correlate it with a caller waiting on its outcome
// elsewhere, or to reference it from another record written in the same
// transaction. Building a Command without calling this is also fine:
// CommandBus.Send and Marshal fill in a missing ID and PublishedAt
// automatically, without ever overwriting one that is already set — so this
// constructor exists purely for callers who need the identity earlier than
// that, not because publishing requires it.
func NewCommandHeader() CommandHeader {
	return CommandHeader{
		ID:          newID(commandIDPrefix),
		PublishedAt: time.Now().UTC(),
	}
}

// NewEventHeader returns an EventHeader with a freshly generated ID and
// PublishedAt set to the current time (UTC), for the same reason
// NewCommandHeader exists for Commands: a caller that needs the Event's
// identity before it is ever published. The prototypical case is an
// aggregate that accumulates several Events as it processes one operation —
// it builds each Event's header with NewEventHeader as the Event is
// recorded, so it can log or correlate them by ID immediately, and only
// hands the finished batch to EventBus.Publish once the operation completes.
// Building an Event without calling this is also fine: EventBus.Publish and
// Marshal fill in a missing ID and PublishedAt automatically, without ever
// overwriting one that is already set.
func NewEventHeader() EventHeader {
	return EventHeader{
		ID:          newID(eventIDPrefix),
		PublishedAt: time.Now().UTC(),
	}
}
