# Context

Glossary for `nats-cqrs`: a simplified, NATS-native CQRS messaging layer.

## Message

The root concept. A Go struct — never `any` — that can travel over the bus. A type
becomes a Message by embedding a marker provided by this library; it cannot be
declared a Message from outside.

Every Message carries a Header and is either a Command or an Event. There is no
third kind, and no Message is both.

## Header

What every Message carries besides its own subject matter: an identity, a moment
of publication, and a place for cross-cutting concerns that belong to the
Message's journey rather than to its meaning — tracing, correlation. The Header
is the single home for such data; nothing travels beside a Message.

## Command

A Message expressing an intent: something that should happen. A Command is
addressed to exactly one logical handler. Two replicas of the same service
compete for a Command; only one of them handles it.

A Command that has not been handled is **work not yet done**, and is kept until
it is handled — or until it is old enough that doing it would no longer be
right.

## Event

A Message stating a fact: something that already happened. An Event is addressed
to anyone interested, and each interested Handler receives its own copy,
independently of the others.

An Event that has been published is **history**. A Handler that comes into
existence later is not owed the past: it begins with what happens next.

## Message Name

The identity of a Message *type*, not of an individual message. Derived from the
Go type by default; a type may override it to decouple its wire identity from its
Go identity. Publisher and subscriber agree on a Message only through this name.

A new version of a Message is a new Message with a new name. Versions do not
convert into one another; they coexist and ignore each other.

## Handler

A named unit of work that consumes one Message type. Its name is part of the
system's contract: it identifies the Handler's position across restarts and
across replicas. Renaming a Handler creates a different Handler — the original's
position is lost.

## Dead Letter

Where a Message goes when a Handler has failed it too many times. A Dead Letter
records which Handler gave up, and why. Because Events fan out, failure belongs
to a Handler and not to a Message: the same Event may be dead for one Handler
and handled by every other.

## Outbox

The durable buffer a Message may pass through on its way out. Publishing into the
Outbox happens in the same unit of work as the state change that caused it, so a
Message can never be emitted for a change that was rolled back, nor lost for a
change that was kept.

The Outbox is defined by what the application must provide — a way to accept
Messages, and a way to hand them over for sending — never by a particular
database. Handing over and sending are a single act: a Message is only
relinquished once it has been sent.

A Message that leaves through the Outbox arrives at least once. Duplicates are
possible and must be harmless.
