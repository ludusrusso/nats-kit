package natscqrs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// ErrUnprocessable is the sentinel a Handler wraps into its returned error
// to signal a permanent failure: the Message it was just handed will never
// succeed, no matter how many times — or how long from now — it is
// retried. A transport that recognizes it (via errors.Is) is expected to
// skip its ordinary retry ladder entirely and dead-letter the Message on
// the spot, on this very first attempt, rather than spend a retry budget
// that exists for problems that might actually go away.
//
// Wrap it — never return it bare — so the concrete error a Handler returns
// still carries whatever specific detail caused the failure:
//
//	if !isValidCustomerID(cmd.CustomerID) {
//		return fmt.Errorf("malformed customer id %q: %w", cmd.CustomerID, natscqrs.ErrUnprocessable)
//	}
//
// What it means: this exact Message, exactly as received, cannot be turned
// into a successful outcome by this Handler — ever. A payload that fails a
// decode step, a value that violates an invariant the domain treats as
// non-negotiable, a business rule that can never become true for this
// particular Message however long this Handler waits: all of these are
// permanent, and ErrUnprocessable is how a Handler says so.
//
// What it does NOT mean: a transient dependency failure. "The card issuer's
// API is down right now", "the database is momentarily unreachable", "the
// card was declined, the customer might succeed with a different one in ten
// minutes" are all ordinary errors — return them unwrapped (or wrapped
// without ErrUnprocessable) and let the transport's normal
// nak-then-redeliver ladder run its course. Wrapping ErrUnprocessable for a
// failure that might succeed on a later attempt throws away retries the
// Message was entitled to.
//
// What happens: the natsjs transport's Runner checks for ErrUnprocessable
// before it ever looks at how many delivery attempts remain. If present, it
// writes the Dead Letter and settles the Message immediately — Attempts:
// 1 — instead of naking it for JetStream to redeliver.
//
// This package's own typedHandler.Handle already wraps ErrUnprocessable for
// every failure it detects before a Handler's registered function is ever
// invoked: an unreadable envelope, a Message Name that does not match what
// the Handler expects, an absent or null payload, or a payload that does
// not unmarshal into the Handler's Message type. None of those can be fixed
// by waiting — they are bugs in the message itself or in whatever produced
// it — so none of them should burn a retry budget meant for problems that
// might actually go away.
var ErrUnprocessable = errors.New("natscqrs: message cannot be processed")

// Kind distinguishes a Handler that consumes Commands from one that
// consumes Events.
type Kind string

const (
	// KindCommand marks a Handler built with NewCommandHandler.
	KindCommand Kind = "command"
	// KindEvent marks a Handler built with NewEventHandler.
	KindEvent Kind = "event"
)

// Handler is a named unit of work that consumes one Message type. Its Name
// is part of the system's contract: a transport identifies the Handler's
// position (e.g. a durable consumer) by that name, across restarts and
// across replicas. Renaming a Handler creates a different Handler — the
// original's position is lost.
type Handler interface {
	// Name is the developer-chosen handler name.
	Name() string
	// Kind reports whether this Handler consumes Commands or Events.
	Kind() Kind
	// Subject is the NATS subject this Handler consumes, derived from the
	// Message type it was built for (see Subject). Once Validate has
	// returned nil, Subject is guaranteed to return the correct subject: a
	// transport must call Validate before ever relying on Subject, e.g. to
	// set up a subscription's filter — an implementation that let a failed
	// derivation surface as "" here, instead of as Validate's error, could
	// silently subscribe to everything.
	Subject() string
	// Validate checks that the Handler is well-formed: that its name is
	// non-empty and that its Message type will produce a valid envelope
	// (header tag, Message Name). A runner must call Validate once, at
	// registration, before subscribing the Handler to anything.
	Validate() error
	// Handle unmarshals data — an envelope, verbatim — checks its name
	// against the Handler's expected Message Name, unmarshals the payload
	// into the Handler's Message type, and invokes the registered function.
	//
	// Every failure Handle can produce on its own, before the registered
	// function is ever invoked, is permanent by construction — a corrupt
	// envelope, a Message Name mismatch, an absent or null payload, a
	// payload that will not unmarshal — so every one of those errors comes
	// back wrapping ErrUnprocessable. The registered function's own
	// returned error is never touched: whether it wraps ErrUnprocessable
	// too is entirely that function's call.
	Handle(ctx context.Context, data []byte) error
}

// typedHandler is the generic implementation behind NewCommandHandler and
// NewEventHandler. T is inferred by the compiler from fn's signature, so
// callers never write it out explicitly.
type typedHandler[T Message] struct {
	name string
	kind Kind
	fn   func(context.Context, T) error

	// subjectOnce guards the one-time resolution of subject/subjectErr, so
	// that whichever of Validate or Subject reaches it first computes it,
	// and every other caller — including a concurrent one — observes the
	// same cached result. Validate calls this on the registration path, so
	// after a successful Validate, Subject is a plain field read: it cannot
	// be wrong. If Subject is called before Validate, it resolves through
	// this same path lazily.
	subjectOnce sync.Once
	subject     string
	subjectErr  error
}

// NewCommandHandler builds a Handler for Command type T, named name, that
// invokes fn for every matching Command it receives. T is inferred from
// fn's signature — NewCommandHandler("charge_order", s.OnChargeOrder)
// compiles with no explicit type argument, as long as OnChargeOrder's
// second parameter is a concrete Command type.
func NewCommandHandler[T Command](name string, fn func(context.Context, T) error) Handler {
	return &typedHandler[T]{name: name, kind: KindCommand, fn: fn}
}

// NewEventHandler builds a Handler for Event type T, named name, that
// invokes fn for every matching Event it receives. T is inferred from fn's
// signature exactly as for NewCommandHandler.
func NewEventHandler[T Event](name string, fn func(context.Context, T) error) Handler {
	return &typedHandler[T]{name: name, kind: KindEvent, fn: fn}
}

func (h *typedHandler[T]) Name() string { return h.name }

func (h *typedHandler[T]) Kind() Kind { return h.kind }

// resolveSubject derives the subject from a zero value of T exactly once,
// caching the result (including a failure) in h.subject/h.subjectErr for
// every later call, from either Validate or Subject.
func (h *typedHandler[T]) resolveSubject() (string, error) {
	h.subjectOnce.Do(func() {
		var zero T
		h.subject, h.subjectErr = Subject(zero)
	})
	return h.subject, h.subjectErr
}

// Subject returns the cached subject resolved by resolveSubject. If Subject
// is called before Validate, it resolves (and caches) the subject lazily,
// through the same code path Validate uses; if that resolution fails, it
// returns "" rather than panicking — Validate is what surfaces the real
// error at registration time. After a successful Validate, resolution
// cannot fail (Validate's own validateMessageType call already proved the
// header tag and Message Name are valid), so Subject cannot be wrong.
func (h *typedHandler[T]) Subject() string {
	subject, err := h.resolveSubject()
	if err != nil {
		return ""
	}
	return subject
}

func (h *typedHandler[T]) Validate() error {
	if h.name == "" {
		return fmt.Errorf("natscqrs: handler name must not be empty")
	}
	if h.fn == nil {
		return fmt.Errorf("natscqrs: handler %q: fn must not be nil", h.name)
	}
	var zero T
	if err := validateMessageType(zero); err != nil {
		return fmt.Errorf("natscqrs: handler %q: %w", h.name, err)
	}
	// Resolve and cache the subject now, so that Subject(), called by a
	// transport after this successful Validate, cannot return a wrong
	// value. This can only fail if zero is somehow neither a Command nor
	// an Event, which validateMessageType above already rules out in
	// practice.
	if _, err := h.resolveSubject(); err != nil {
		return fmt.Errorf("natscqrs: handler %q: %w", h.name, err)
	}
	return nil
}

// Handle's own failures — everything returned before h.fn is ever
// invoked — all wrap ErrUnprocessable: an envelope this package itself did
// not produce, a Message Name that does not match, or a payload that is
// absent, null or will not unmarshal are never going to succeed by being
// retried. Each %w wraps both the underlying cause (preserved exactly as
// before, for callers already inspecting it) and ErrUnprocessable — Go
// supports more than one %w per fmt.Errorf call, and errors.Is/As walks
// every wrapped error, so nothing about the existing chain is lost.
func (h *typedHandler[T]) Handle(ctx context.Context, data []byte) error {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("natscqrs: handler %q: failed to unmarshal envelope: %w: %w", h.name, err, ErrUnprocessable)
	}

	var zero T
	wantName, err := MessageName(zero)
	if err != nil {
		return fmt.Errorf("natscqrs: handler %q: %w: %w", h.name, err, ErrUnprocessable)
	}
	if env.Name != wantName {
		return fmt.Errorf(
			"natscqrs: handler %q: envelope name %q does not match expected message name %q: %w",
			h.name, env.Name, wantName, ErrUnprocessable,
		)
	}

	var v T

	// json.Unmarshal([]byte("null"), &v) is a documented no-op: it returns
	// nil and leaves v untouched. An absent or literal-null payload would
	// therefore silently produce a zero-valued T — empty ID, zero
	// PublishedAt, zero domain fields — and hand it to fn with no error.
	// Reject both cases explicitly instead of letting them through.
	if len(env.Payload) == 0 || bytes.Equal(bytes.TrimSpace(env.Payload), []byte("null")) {
		return fmt.Errorf("natscqrs: handler %q: envelope payload is absent or null, want a %T: %w", h.name, v, ErrUnprocessable)
	}

	if err := json.Unmarshal(env.Payload, &v); err != nil {
		return fmt.Errorf("natscqrs: handler %q: failed to unmarshal payload into %T: %w: %w", h.name, v, err, ErrUnprocessable)
	}
	return h.fn(ctx, v)
}
