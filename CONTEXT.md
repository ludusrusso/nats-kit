# Context

Glossary for `nats-kit`: simplified, NATS-native messaging — a CQRS bus,
Cron Jobs and Durable Jobs — and a Cache.

The glossary covers all four. The CQRS bus owns everything from Message to
Aggregate below; the Cron Job and the Durable Job stand beside it, and the
terms they need follow after. Those three share a broker and the Namespace
that names their owner — nothing else. The Cache comes last and shares even
less: the broker, and nothing beside it.

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

## Aggregate

A domain object that decides and, having decided, records the Events its
decision produced — in the order it produced them, and without publishing any
of them itself.

An Event an Aggregate has recorded is **not history yet**: it becomes history
when whoever persists the Aggregate drains it, taking the recorded Events for
publication and leaving the Aggregate with nothing left to take. Recording and
draining are two acts with two owners, and everything else follows from that:
an Aggregate drained twice states its facts once, and an Aggregate never
drained states them to nobody.

## Namespace

The owner of a set of jobs: the name a service claims so that its Cron Jobs
and its Durable Jobs are recognisably its own. A Namespace is part of every
subject and of every consumer name, so two services that claim different
Namespaces can declare a job of the same name without ever meeting, while
every instance of one service must claim the same Namespace to share its
work.

## Cron Job

A named unit of recurring work, declared in code and scheduled by the
broker. Every instance of the service that declares it binds to the same
position, so a firing runs on exactly one of them.

A Cron Job never overlaps itself: the next firing waits for the previous one
to finish. A failed run is not retried — the job simply runs again at its
next firing.

## Schedule

When a Cron Job fires, written as an interval, a predefined alias, or a cron
expression.

A Schedule lives on the server, not in the process: it survives restarts and
deploys, and changing the expression is what — and all that — moves the
cadence.

## Tick

One firing of a Schedule, delivered to a single instance of its Cron Job. A
Tick says when the execution was due and when the next one falls due; it is
informative only.

A Tick nobody collects in time expires rather than accumulating.

## Durable Job

Long-running imperative work that outlives the request that asked for it. A
Durable Job is one message the broker holds open: delivered to exactly one
instance, kept until that instance reports the work fully done, redelivered
from the beginning if the instance dies.

Because every delivery re-runs the job from the start, its remaining work
must be re-derivable from durable state. The message identifies what to do,
never how far it got.

## Dispatch

Starting a Durable Job: publishing the single message that is the job.

The dispatching service need not own the job — it names the Namespace the
job belongs to — and need not wait for it. A dispatched job is durable from
the moment it is published, whether or not any instance is running yet.

## Cache

A shared memory of an answer that was expensive to obtain, kept under a key
so that asking again is cheap. A Cache holds one kind of value, and every
instance of every service bound to it sees the same entries.

A Cache is an optimisation and never a source of truth: an entry may be
absent at any moment, and everything a Cache does when asked for one it does
not have is ask for it again. It therefore never remembers a failure — a
failure is not an answer.

## Substrate

Where a Cache keeps its entries, and the sole owner of how long they live: a
Substrate carries one lifetime, shared by every entry in it. Nothing about a
single entry can be older or younger than that.

A Substrate that cannot be reached does not break the Cache: the answer is
obtained the expensive way and returned unremembered, so an outage costs
time and never correctness.

## Loader

How an answer is obtained when the Cache does not hold it. Callers asking for
the same key at the same time share a single Loader — that is what a Cache
saves, beyond the entries it holds.

## Hit Validator

An optional test an entry must pass to count as an answer, for values that
can die before their Substrate's lifetime says they should. An entry that
fails it is not an answer at all: it is obtained again and replaces the one
that failed.
