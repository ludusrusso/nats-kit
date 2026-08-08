# nats-cqrs

This repository holds three sibling NATS-native primitives — the CQRS bus,
Cron Jobs and Durable Jobs — that share nothing but the embedded test
server, each owning its own JetStream stream.

`nats-cqrs` is a small CQRS messaging layer over NATS JetStream, in Go. JSON
only, subjects derived from Go type names: a Command goes to exactly one
Handler, an Event fans out to every Handler that wants it. An optional
Outbox lets a Message be published in the same transaction as the state
change that caused it, so a Message is never emitted for a change that gets
rolled back. It lives in `cqrs`, with its JetStream transport in
`cqrs/natsjs` and the Outbox in `cqrs/outbox`.

```
go get github.com/ludusrusso/nats-cqrs
```

## Example

The snippet below is a trimmed version of
[`cqrs/natsjs/example_test.go`](cqrs/natsjs/example_test.go) — a runnable
`go test` example, on top of `natstest`'s embedded JetStream server, so it
needs no infrastructure of its own. Run it with
`go test -run Example ./cqrs/natsjs/`.

```go
// PlaceOrder is a Command: an intent addressed to exactly one Handler.
// The `json:"header"` tag on the embedded header is mandatory — see
// "Declaring Messages" below for why.
type PlaceOrder struct {
	cqrs.CommandHeader `json:"header"`
	CustomerID         string `json:"customer_id"`
}

// OrderPlaced is an Event: a fact, fanned out to every interested Handler.
type OrderPlaced struct {
	cqrs.EventHeader `json:"header"`
	OrderID          string `json:"order_id"`
	CustomerID       string `json:"customer_id"`
}

// The Outbox. EventBus.Publish below writes here — in a real service,
// inside the same database transaction as the work that produced the
// Event — instead of to NATS directly.
mem := outbox.NewMemory()
events := cqrs.NewEventBus(mem)

pub, _ := natsjs.NewPublisher(ctx, nc)
commands := cqrs.NewCommandBus(pub)

placeOrder := cqrs.NewCommandHandler("place_order", func(ctx context.Context, cmd PlaceOrder) error {
	// The Handler's "work": a real service would touch its own database
	// here. This is the exact same events.Publish call a Handler would
	// make if events had been built with NewEventBus(pub) instead of
	// NewEventBus(mem) — only the Sink underneath changes.
	return events.Publish(ctx, OrderPlaced{
		EventHeader: cqrs.NewEventHeader(),
		OrderID:     "order-1",
		CustomerID:  cmd.CustomerID,
	})
})

notifyCustomer := cqrs.NewEventHandler("notify_customer", func(ctx context.Context, evt OrderPlaced) error {
	fmt.Println("notify customer", evt.CustomerID, "about order", evt.OrderID)
	return nil
})

runner, _ := natsjs.New(ctx, nc, "orders-svc")
runner.Register(placeOrder, notifyCustomer)
go runner.Run(ctx)

commands.Send(ctx, PlaceOrder{CustomerID: "cust-1"})

// Drain the Outbox into NATS. A long-running service runs a Forwarder's
// Run loop in its own goroutine; Once is the same drain, invoked by hand.
forwarder := outbox.NewForwarder(mem, pub)
forwarder.Once(ctx)
```

## What this is not

- Not an event store. There is no replay, no append-only log a Handler
  reads back — once delivered, `nats-cqrs` is done with a Message.
- No read models or projections. Building one is ordinary application
  code: an Event Handler that writes to your own database.
- No request/reply. A Command is fire-and-forget; `CommandBus.Send` reports
  only whether sending succeeded, never a Handler's outcome.
- No schema registry, no encoding but JSON.

It exists because [Watermill](https://watermill.io/)'s CQRS component is
broker-agnostic, and therefore reimplements in Go a number of things
JetStream already does natively — work-queue competition, fan-out,
redelivery, deduplication. This library assumes JetStream and gets those for
free, at the cost of not supporting any other broker.

## Declaring Messages

A Message is a plain Go struct that embeds `cqrs.CommandHeader` or
`cqrs.EventHeader` — never both, and every Message is one or the other,
never neither. The embedded field **must** carry an explicit `json:"header"`
tag:

```go
type CreateOrder struct {
	cqrs.CommandHeader `json:"header"`
	CustomerID         string `json:"customer_id"`
}
```

Forgetting the tag is the single most likely mistake a new user will make,
and it is dangerous precisely because `encoding/json` will not tell you:
without the tag, Go promotes the header's `ID`/`PublishedAt`/`Metadata`
fields inline into the message's own JSON, and a domain field whose tag
happens to collide with `"id"`, `"published_at"` or `"metadata"` silently
destroys the message's identity — no panic, no error, just a broken
envelope on the wire. `nats-cqrs` detects this at validation time (every
`Handler` is checked at `Register`, every outgoing `Message` at `Marshal`)
and refuses to start rather than let it happen.

## Message names and subjects

A Message's wire identity — its **Message Name** — is `package.Struct` by
default (the Go type name, last package path segment only). It is
overridable by implementing `Type() string`:

```go
func (CreateOrder) Type() string { return "orders.create" }
```

The Message Name becomes the NATS subject: `commands.<name>` for a Command,
`events.<name>` for an Event.

Versioning is by naming, not by schema migration: a new version of a
Message is a new Go type with a new name. The old and new versions coexist
and ignore each other — there is no built-in conversion between them, by
design.

## The `Sink` seam

Everything that leaves a process through this library goes through one
interface:

```go
type Sink interface {
	Publish(ctx context.Context, records ...Record) error
}
```

`natsjs.Publisher` implements it by writing straight to NATS JetStream. A
database-backed Outbox implements it by appending to a table inside the
caller's own transaction. `CommandBus` and `EventBus` are built from nothing
but a `Sink`, so domain code that holds one can never tell, and never needs
to tell, which kind it has:

```go
// Inside a database transaction, via an Outbox:
events := cqrs.NewEventBus(myOutboxSink)
// Straight to NATS:
events := cqrs.NewEventBus(natsPublisher)
// Either way, the call below is identical.
events.Publish(ctx, OrderCreated{OrderID: id})
```

This is the seam the whole design turns on, and the easiest thing to miss
from reading the code alone.

## The Outbox

The `cqrs/outbox` package implements the Outbox half of the Sink seam. An
application provides exactly two things:

1. A `cqrs.Sink` bound to its own transaction, used to accept Messages.
2. An `outbox.Reader`, whose one method, `ReadForSend`, hands pending
   Records over for sending as a single, indivisible act — see
   [`outbox.Reader`'s doc comment](cqrs/outbox/reader.go) for the full
   transactional contract (it is long and precise on purpose: every
   implementer needs to get it exactly right) and illustrative SQL.

`outbox.Memory` is a dependency-free, in-memory implementation of both,
used above and throughout this repo's own tests — it is executable
documentation of the contract, not just a mock. `outbox.Forwarder` is the
loop that drains a `Reader` into a `Sink`; nothing starts it automatically,
because where it runs (a goroutine, a sidecar, a cron job calling `Once`
directly) is a deployment decision left to the caller.

Delivery through the Outbox is **at-least-once**: a publish can succeed and
the transaction that relinquishes it can still fail to commit, in which
case the Record is sent again on the next pass. Handlers must be
idempotent; duplicates must be harmless.

## Failure handling

There is no application-level retry loop anywhere in this library. Once a
Handler returns an error, JetStream redelivers it later according to the
consumer's `AckWait`/`MaxDeliver` settings — that is all the Runner does.

After `MaxDeliver` attempts, the Runner writes a `cqrs.DeadLetter` to
`dlq.<original subject>`, recording **which Handler** gave up, its last
error, and how many attempts it took. Because Events fan out, giving up
belongs to a Handler, not to a Message: the same Event can be dead for one
Handler and handled successfully by every other.

Wrap `cqrs.ErrUnprocessable` into a returned error to skip the retry
ladder entirely and dead-letter on the very first attempt, for a failure no
amount of waiting could ever fix:

```go
if !isValidCustomerID(cmd.CustomerID) {
	return fmt.Errorf("malformed customer id %q: %w", cmd.CustomerID, cqrs.ErrUnprocessable)
}
```

## Operational notes

| Stream          | Subjects    | Retention  | Default max age | Dedup window |
|-----------------|-------------|------------|------------------|--------------|
| `cqrs-commands` | `commands.>`| work queue | 24h              | 2m           |
| `cqrs-events`   | `events.>`  | limits     | 7d               | 2m           |
| `cqrs-dlq`      | `dlq.>`     | limits     | 30d              | 2m           |

Every durable consumer is named `serviceName_handlerName`. Two consequences
of that naming follow, and only one of them fails loudly:

- Renaming a **Command** Handler is a startup failure: JetStream rejects
  the new durable while the old one still holds the same filter subject on
  the (work-queue) commands stream. Delete the old durable before deploying
  the rename.
- Renaming an **Event** Handler is silent: the new durable is created fresh
  with `DeliverNew`, and everything that was pending under the old name is
  skipped, not migrated.

See [`docs/adr/0001-commands-expire-and-new-consumers-start-from-now.md`](docs/adr/0001-commands-expire-and-new-consumers-start-from-now.md)
for why the two streams differ, and
[`docs/adr/0002-outbox-hands-over-and-sends-as-one-act.md`](docs/adr/0002-outbox-hands-over-and-sends-as-one-act.md)
for the Outbox's transactional contract — both would otherwise look
arbitrary.

## Cron Jobs

The `cronjob` package runs recurring work whose Schedule is kept by the NATS
server, as a JetStream Scheduled Message, rather than by an in-process
ticker. Every replica declares the same jobs and binds to the same durable
consumer, so every replica can run and a firing is still delivered to
exactly one of them.

```go
runner, _ := cronjob.New(ctx, nc, "billing")

runner.Register(
	cronjob.NewJob("sweep_expired", "@every 5m", func(ctx context.Context, tick cronjob.Tick) error {
		return sweepExpired(ctx)
	}),
	cronjob.NewJob("nightly_report", "0 0 3 * * *", func(ctx context.Context, tick cronjob.Tick) error {
		return report(ctx, tick.ScheduledAt)
	}),
)

go runner.Run(ctx)
```

A `Tick` carries `ScheduledAt`, when the execution was due, and `NextAt`,
when the next one falls due. Both are informative; nothing the handler must
do depends on them, and `NextAt` is the zero time when the server
advertises none.

A Schedule is written in one of three forms, all validated at `Register`:

| Form            | Example       | Notes                                                                    |
|-----------------|---------------|--------------------------------------------------------------------------|
| interval        | `@every 5m`   | parsed by `time.ParseDuration`; must be at least `1s`                     |
| alias           | `@daily`      | `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` — no others exist    |
| cron expression | `0 0 3 * * *` | six fields: second minute hour day-of-month month day-of-week             |

A five-field cron expression is rejected. `Register` only counts a cron
expression's fields; the server is what rejects a field whose contents are
malformed.

| Stream     | Subjects                                                          | Consumer                             |
|------------|-------------------------------------------------------------------|--------------------------------------|
| `cronjobs` | `cronjobs.schedules.<ns>.<job>` → `cronjobs.ticks.<ns>.<job>`      | `cron_<ns>_<job>`, `MaxAckPending: 1` |

`MaxAckPending: 1` is what stops a job overlapping itself cluster-wide: the
next Tick is not delivered anywhere until the previous one is acked. A Tick
is acked with an `AckWait` of 5 minutes, so an instance that dies mid-run
has its Tick redelivered to a survivor rather than losing the firing.

A handler error is logged and the Tick is acked anyway. There is no
redelivery for a failed run — the job simply runs again at its next Tick. A
Tick that goes uncollected expires rather than accumulating: a Schedule
written as an interval or an alias carries a per-message TTL of five times
that interval. A cron expression has no single interval, so its Ticks keep
the server's default TTL.

Startup reconciles the broker to what the code declares, within the
Runner's own namespace only: a schedule for a job the code no longer
declares is purged, its consumer deleted, and another namespace's jobs are
never touched. A Schedule is republished only when its expression changed,
so a restart that changes nothing moves nothing.

Cron Jobs need a NATS server that supports Scheduled Messages — 2.14 or
later. There is no in-process fallback, and no runtime version check: an
older server simply refuses the stream.

## Durable Jobs

The `durablejob` package runs long-running work where the message *is* the
continuation. One message on the job subject is one job, held open by the
broker — delivered to one instance, unacked for as long as it takes — until
that instance reports the work fully done.

```go
runner, _ := durablejob.New(ctx, nc, "billing")

runner.Register(
	durablejob.NewJob("reindex_tenant", func(ctx context.Context, payload []byte) error {
		return reindex(ctx, string(payload))
	}),
)

go runner.Run(ctx)

// Dispatching is separate, and need not happen in the owning service:
// a Dispatcher holds no namespace of its own, it names one per call.
dispatcher, _ := durablejob.NewDispatcher(ctx, nc)
dispatcher.Dispatch(ctx, "billing", "reindex_tenant", []byte("tenant-1"))
```

`Execute` must be re-runnable, because a redelivery re-runs it from the
beginning. It has to re-derive its remaining work from durable state — rows
still matching a predicate, objects still missing — and the payload must
identify the work without ever carrying progress. Nothing in the library
enforces this; it is the one rule a caller has to keep.

While `Execute` runs, the worker sends `InProgress` heartbeats every half
`AckWait`, so a job that is merely still working never burns a delivery. A
returned error is therefore a genuine failure, and is redelivered. A panic
is treated exactly like a returned error. An error returned while the
Runner's context is already cancelled is naked without a verdict: shutting
down is not a judgement on the job.

| Option           | Default                     | What it sets                                            |
|------------------|-----------------------------|---------------------------------------------------------|
| `WithAckWait`    | `DefaultAckWait`, 5m        | how long a delivery may go unacked before redelivery     |
| `WithMaxDeliver` | `DefaultMaxDeliver`, 5      | genuine failures before the job is parked on the DLQ     |
| `WithLogger`     | `slog.Default()`            | where a failure and a dead-letter are logged             |

| Stream         | Subjects                                                        | Consumer            |
|----------------|------------------------------------------------------------------|---------------------|
| `durablejobs`  | `durablejobs.jobs.<ns>.<job>`, `durablejobs.dlq.<ns>.<job>`      | `job_<ns>_<job>`    |

A Runner pulls one message at a time, so an instance runs one job at a time.
The consumer's server-side `MaxDeliver` is set one above the configured
value: that extra delivery is the retry window for the dead-letter hand-off
itself, not another attempt at the job.

After `MaxDeliver` genuine failures the job is parked on its DLQ subject —
in the same `durablejobs` stream — carrying the original payload and four
headers:

| Header                | Value                                         |
|-----------------------|-----------------------------------------------|
| `Job-Origin-Subject`  | the job subject it was dispatched on          |
| `Job-Origin-Sequence` | its sequence in the stream                    |
| `Job-Deliveries`      | how many deliveries it took to give up        |
| `Job-Error`           | the last error `Execute` returned             |

Unlike the three `cqrs-*` streams, which `natsjs.EnsureStreams` creates only
when missing and never reconfigures, the `cronjobs` and `durablejobs`
streams are wholly owned by their packages and are converged with
`CreateOrUpdateStream` on every start. The difference is ownership, not
taste; [ADR 0003](docs/adr/0003-cron-jobs-on-jetstream-scheduled-messages.md)
and [ADR 0004](docs/adr/0004-durable-jobs-redeliver-until-done.md) record
why.

## Testing

`natstest.Start(t)` starts a real, embedded, JetStream-enabled `nats-server`
per test and returns a connection plus a cleanup function. It is the one
thing all three primitives share:

```go
nc, cleanup := natstest.Start(t)
defer cleanup()
```

There is deliberately no in-memory fake of the bus: work-queue competition,
fan-out, redelivery and durable position are all JetStream's behavior, not
this library's, and a fake that does not reproduce them would give false
confidence about exactly the hard parts.

## Further reading

- [`CONTEXT.md`](CONTEXT.md) — the project glossary (Message, Header,
  Command, Event, Handler, Dead Letter, Outbox, Cron Job, Schedule, Tick,
  Durable Job, Dispatch, Namespace). The vocabulary above follows it
  throughout.
- [`docs/adr/`](docs/adr/) — the four decisions referenced above, recorded
  in full.

## License

[0BSD](LICENSE) — do whatever you want with it.
