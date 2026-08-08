package cqrs

import "context"

// CommandBus is a thin typed facade over a Publisher for sending Commands.
type CommandBus struct {
	sink Publisher
}

// NewCommandBus returns a CommandBus that hands every Command it sends to
// sink.
func NewCommandBus(sink Publisher) *CommandBus {
	return &CommandBus{sink: sink}
}

// Send hydrates, validates and marshals every Command into a Record (see
// Marshal), then hands the whole batch to the Publisher in a single call. A
// single call matters when the Publisher is an Outbox: the batch then lands
// in one transaction, so it is either entirely written or not at all.
//
// A Command is addressed to exactly one logical handler and is
// fire-and-forget from the sender's point of view: Send returns an error
// only if marshaling or publishing itself failed. It never reports, and has
// no way to report, a handler's outcome — a Command that has been sent is
// work handed off, not work awaited.
func (b *CommandBus) Send(ctx context.Context, cmds ...Command) error {
	if len(cmds) == 0 {
		return nil
	}
	records := make([]Record, 0, len(cmds))
	for _, c := range cmds {
		r, err := Marshal(c)
		if err != nil {
			return err
		}
		records = append(records, r)
	}
	return b.sink.Publish(ctx, records...)
}

// EventBus is a thin typed facade over a Publisher for publishing Events.
type EventBus struct {
	sink Publisher
}

// NewEventBus returns an EventBus that hands every Event it publishes to
// sink.
func NewEventBus(sink Publisher) *EventBus {
	return &EventBus{sink: sink}
}

// Publish hydrates, validates and marshals every Event into a Record (see
// Marshal), then hands the whole batch to the Publisher in a single call, for
// the same batch-is-a-transaction reason as CommandBus.Send.
//
// An Event states a fact addressed to anyone interested; once published, it
// is history. Publish returns an error only if marshaling or publishing
// itself failed.
func (b *EventBus) Publish(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	records := make([]Record, 0, len(events))
	for _, e := range events {
		r, err := Marshal(e)
		if err != nil {
			return err
		}
		records = append(records, r)
	}
	return b.sink.Publish(ctx, records...)
}
