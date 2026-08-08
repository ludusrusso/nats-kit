// Package outbox provides the transactional-outbox half of nats-cqrs: the
// durable buffer a Message may pass through on its way out, so that
// emitting a Message can never race the database change that caused it.
// See the "Outbox" entry in CONTEXT.md at the repository root for the
// vocabulary, and ADR 0002 (docs/adr/0002-outbox-hands-over-and-sends-as-one-act.md)
// for the semantics this package implements to the letter.
//
// This package has zero external dependencies beyond the standard library
// and the root natscqrs package: no database driver, no SQL, and no NATS.
// That absence is the entire point of the design — this library never
// learns what database, or what messaging transport, an application uses.
//
// # What the application provides
//
// The Outbox is defined by two things the application must provide, never
// by a particular database:
//
//   - A way to accept Messages: natscqrs.Sink, implemented by something the
//     application constructs bound to a transaction it already opened, so
//     this library never sees that transaction handle. This package does
//     not redeclare Sink — reusing the root package's own transport seam is
//     exactly what makes an Outbox and a direct NATS publisher
//     interchangeable to domain code (see "One Sink, two meanings" below).
//   - A way to hand Messages over for sending: Reader (see reader.go).
//     ReadForSend hands over and sends as a single, indivisible act,
//     because a Message is only relinquished once it has actually been
//     sent — the Outbox glossary entry's central rule.
//
// # One Sink, two meanings
//
// Domain code should never need to know, or care, whether it is writing
// into an Outbox or publishing straight to NATS. natscqrs.CommandBus and
// natscqrs.EventBus already provide exactly that symmetry, because both are
// built from nothing but a Sink. This package therefore does not, and must
// not, define its own bus types — doing so would duplicate a symmetry the
// root package already gives away for free. Compare:
//
//	// Inside a database transaction: publish through the Outbox. sink is
//	// bound to tx, and this package's Reader will later drain what lands
//	// here — in the same transaction as the state change that caused it.
//	sink := myapp.NewTxOutboxSink(tx)
//	events := natscqrs.NewEventBus(sink)
//	events.Publish(ctx, OrderCreated{OrderID: id})
//
//	// Outside any transaction: publish straight to NATS.
//	sink := natsPublisher // e.g. a JetStream-backed natscqrs.Sink
//	events := natscqrs.NewEventBus(sink)
//	events.Publish(ctx, OrderCreated{OrderID: id})
//
// Both calls to events.Publish above are the exact same code. Nothing about
// EventBus changes; only the Sink handed to NewEventBus at construction
// time does — that single substitution is what lets a service move a given
// Event from "published in-transaction, via the Outbox" to "published
// directly" (or back) without touching the code that decided to publish it.
//
// # Draining the Outbox
//
// Forwarder is the loop that drains a Reader into a natscqrs.Sink — see
// forwarder.go. It never starts itself: where it runs is a deployment
// decision left entirely to the caller.
//
// Memory is a dependency-free, in-memory implementation of both Sink and
// Reader — see memory.go. It makes this package (and code built on it)
// testable without a real database, and its comments walk through exactly
// which line implements which step of the Reader contract.
package outbox
