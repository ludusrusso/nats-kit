package natscqrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// envelope is the wire format every Message is published as:
//
//	{"name": "<Message Name>", "payload": <the domain struct as JSON>}
//
// The payload is exactly json.Marshal of the user's struct, so it carries
// the nested "header" object plus the domain fields. No other metadata
// travels anywhere else: everything cross-cutting lives in Header.Metadata.
type envelope struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

// Record is a single Message, ready to leave the process: an envelope
// serialized to bytes, addressed to a subject, carrying the Message's own
// ID for deduplication.
type Record struct {
	// Subject is the NATS subject to publish to: "commands.<name>" or
	// "events.<name>".
	Subject string
	// ID is the Message's own ID. The NATS publisher sends it as the
	// Nats-Msg-Id header so JetStream can deduplicate a republished
	// Record — which is what makes at-least-once delivery through the
	// Outbox harmless.
	ID string
	// Data is the envelope, marshaled to JSON, verbatim.
	Data []byte
}

// Sink accepts Records for delivery. It is the one seam this whole design
// turns on: the exact same Sink is implemented both by a publisher that
// writes directly to NATS and by an application's database Outbox, and
// domain code that only holds a Sink never knows, and never needs to know,
// which one it has.
//
// A call to Publish carries a whole batch, so that a batch stays a batch:
// when the Sink is an Outbox, the batch lands in one transaction, and is
// therefore either entirely written or not at all.
type Sink interface {
	Publish(ctx context.Context, records ...Record) error
}

// headerReader is implemented by CommandHeader and EventHeader via a value
// receiver, so any Message value — even a non-addressable one obtained
// straight from an interface — satisfies it through Go's ordinary method
// promotion for embedded fields. No reflection is needed to read an ID back
// out of a Message.
type headerReader interface {
	headerID() string
}

func (h CommandHeader) headerID() string { return h.ID }
func (h EventHeader) headerID() string   { return h.ID }

// Marshal hydrates m's header (see hydrateHeader — a missing ID or
// PublishedAt is filled in, m itself is never mutated), validates m's type,
// and serializes the result into a Record ready to hand to a Sink.
//
// Marshal is exported because it is not only used internally by CommandBus
// and EventBus: an Outbox implementation accepts Messages directly from
// domain code, before they ever reach a bus, and needs the exact same
// Message-to-Record conversion.
func Marshal(m Message) (Record, error) {
	if m == nil {
		return Record{}, errors.New("natscqrs: cannot marshal a nil Message")
	}
	if err := validateHeaderTag(reflect.TypeOf(m)); err != nil {
		return Record{}, err
	}
	name, err := MessageName(m)
	if err != nil {
		return Record{}, err
	}
	prefix, err := subjectPrefix(m)
	if err != nil {
		return Record{}, err
	}

	hydrated := hydrateHeader(m)

	payload, err := json.Marshal(hydrated)
	if err != nil {
		return Record{}, fmt.Errorf("natscqrs: %s: failed to marshal payload: %w", name, err)
	}
	data, err := json.Marshal(envelope{Name: name, Payload: payload})
	if err != nil {
		return Record{}, fmt.Errorf("natscqrs: %s: failed to marshal envelope: %w", name, err)
	}

	// This assertion cannot fail in practice: hydrated is a Message, and
	// Message is sealed to CommandHeader and EventHeader, both of which
	// implement headerReader with a value receiver (see above). It is
	// checked anyway, and turned into a returned error rather than a
	// silent empty ID, because ID becomes the Nats-Msg-Id header a
	// transport deduplicates on: an empty ID would silently defeat
	// deduplication for a Record that otherwise looks entirely normal,
	// which is worse than failing loudly here.
	hr, ok := hydrated.(headerReader)
	if !ok {
		return Record{}, fmt.Errorf("natscqrs: %s: message does not expose a header ID", name)
	}

	return Record{Subject: prefix + name, ID: hr.headerID(), Data: data}, nil
}
