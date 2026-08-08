// Package natsjs is the NATS JetStream transport for
// github.com/ludusrusso/nats-cqrs: it provisions the streams Commands and
// Events travel through, publishes Records to them (Publisher, a
// natscqrs.Sink), and runs registered Handlers against them (Runner).
//
// The vocabulary used throughout this package follows the same project
// glossary as the root package — see CONTEXT.md at the repository root —
// plus the delivery-policy decisions recorded in
// docs/adr/0001-commands-expire-and-new-consumers-start-from-now.md. Read
// both before changing anything here: several choices that look arbitrary
// in isolation (DeliverAll for Commands, DeliverNew for Events, the 24h age
// limit on the commands stream, NakWithDelay instead of a bare Nak) are
// direct consequences of that ADR — see runHandler's DeliverPolicy comment
// for why Commands specifically use DeliverAll rather than the DeliverNew
// the ADR calls for uniformly: a work-queue stream (which the commands
// stream is) rejects DeliverNew outright (server error code 10101), so
// DeliverAll plus the commands stream's MaxAge is how this package honors
// the ADR's intent for Commands in practice.
//
// # Streams vs. consumers
//
// This package draws a hard line between the three streams and the
// consumers built on top of them:
//
//   - Streams (cqrs-commands, cqrs-events, cqrs-dlq) are shared
//     infrastructure. EnsureStreams creates them if and only if they do not
//     already exist, and never reconfigures one that does — see
//     EnsureStreams's doc comment for why.
//   - Consumers belong to a single Handler: this package creates and
//     configures them freely, because a consumer is born and dies with the
//     code that registered its Handler.
package natsjs
