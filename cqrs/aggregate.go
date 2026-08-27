package cqrs

import "context"

// Aggregate is the embeddable buffer of the Events a domain object has
// recorded and not yet published; whoever persists the object drains it
// (ADR 0006).
type Aggregate struct {
	events []Event
}

// Record buffers one Event as a fact the aggregate has just produced.
func (a *Aggregate) Record(e Event) {
	a.events = append(a.events, e)
}

// PullEvents hands over the Events recorded since the last pull, in recording
// order, and forgets them — so a second pull, like a second Drain, yields
// nothing.
func (a *Aggregate) PullEvents() []Event {
	events := a.events
	a.events = nil
	return events
}

// EventSource is anything that hands over the Events it has recorded, which in
// practice is anything embedding Aggregate.
type EventSource interface {
	PullEvents() []Event
}

// Drain publishes everything src has recorded through sink, in recording
// order, and leaves src with nothing left to pull.
//
// sink decides where those Events go, and it is a parameter rather than
// something Drain finds for itself: a Publisher bound to the caller's
// transaction takes them out through the Outbox, a transport's Publisher sends
// them straight out, and src can tell neither apart. A failed Drain has
// already pulled them, so what a caller retries is the whole unit of work,
// never the Drain alone. Events only, deliberately: an aggregate states facts
// about itself, while a Command is an intent addressed elsewhere and is sent
// by whoever forms it (ADR 0006).
func Drain(ctx context.Context, sink Publisher, src EventSource) error {
	return NewEventBus(sink).Publish(ctx, src.PullEvents()...)
}
