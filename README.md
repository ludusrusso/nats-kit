# nats-cqrs

`nats-cqrs` is a small CQRS messaging layer over NATS JetStream, in Go. JSON
only, subjects derived from Go type names: a Command goes to exactly one
Handler, an Event fans out to every Handler that wants it. An optional
Outbox lets a Message be published in the same transaction as the state
change that caused it, so a Message is never emitted for a change that gets
rolled back.

```
go get github.com/ludusrusso/nats-cqrs
```

## Example

The snippet below is a trimmed version of
[`natsjs/example_test.go`](natsjs/example_test.go) — a runnable `go test`
example, on top of `natstest`'s embedded JetStream server, so it needs no
infrastructure of its own. Run it with `go test -run Example ./natsjs/`.

```go
// PlaceOrder is a Command: an intent addressed to exactly one Handler.
// The `json:"header"` tag on the embedded header is mandatory — see
// "Declaring Messages" below for why.
type PlaceOrder struct {
	natscqrs.CommandHeader `json:"header"`
	CustomerID             string `json:"customer_id"`
}

// OrderPlaced is an Event: a fact, fanned out to every interested Handler.
type OrderPlaced struct {
	natscqrs.EventHeader `json:"header"`
	OrderID              string `json:"order_id"`
	CustomerID           string `json:"customer_id"`
}

// The Outbox. EventBus.Publish below writes here — in a real service,
// inside the same database transaction as the work that produced the
// Event — instead of to NATS directly.
mem := outbox.NewMemory()
events := natscqrs.NewEventBus(mem)

pub, _ := natsjs.NewPublisher(ctx, nc)
commands := natscqrs.NewCommandBus(pub)

placeOrder := natscqrs.NewCommandHandler("place_order", func(ctx context.Context, cmd PlaceOrder) error {
	// The Handler's "work": a real service would touch its own database
	// here. This is the exact same events.Publish call a Handler would
	// make if events had been built with NewEventBus(pub) instead of
	// NewEventBus(mem) — only the Sink underneath changes.
	return events.Publish(ctx, OrderPlaced{
		EventHeader: natscqrs.NewEventHeader(),
		OrderID:     "order-1",
		CustomerID:  cmd.CustomerID,
	})
})

notifyCustomer := natscqrs.NewEventHandler("notify_customer", func(ctx context.Context, evt OrderPlaced) error {
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

A Message is a plain Go struct that embeds `natscqrs.CommandHeader` or
`natscqrs.EventHeader` — never both, and every Message is one or the other,
never neither. The embedded field **must** carry an explicit `json:"header"`
tag:

```go
type CreateOrder struct {
	natscqrs.CommandHeader `json:"header"`
	CustomerID string `json:"customer_id"`
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
events := natscqrs.NewEventBus(myOutboxSink)
// Straight to NATS:
events := natscqrs.NewEventBus(natsPublisher)
// Either way, the call below is identical.
events.Publish(ctx, OrderCreated{OrderID: id})
```

This is the seam the whole design turns on, and the easiest thing to miss
from reading the code alone.

## The Outbox

The `outbox` package implements the Outbox half of the Sink seam. An
application provides exactly two things:

1. A `natscqrs.Sink` bound to its own transaction, used to accept Messages.
2. An `outbox.Reader`, whose one method, `ReadForSend`, hands pending
   Records over for sending as a single, indivisible act — see
   [`outbox.Reader`'s doc comment](outbox/reader.go) for the full
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

After `MaxDeliver` attempts, the Runner writes a `natscqrs.DeadLetter` to
`dlq.<original subject>`, recording **which Handler** gave up, its last
error, and how many attempts it took. Because Events fan out, giving up
belongs to a Handler, not to a Message: the same Event can be dead for one
Handler and handled successfully by every other.

Wrap `natscqrs.ErrUnprocessable` into a returned error to skip the retry
ladder entirely and dead-letter on the very first attempt, for a failure no
amount of waiting could ever fix:

```go
if !isValidCustomerID(cmd.CustomerID) {
	return fmt.Errorf("malformed customer id %q: %w", cmd.CustomerID, natscqrs.ErrUnprocessable)
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

## Testing

`natstest.Start(t)` starts a real, embedded, JetStream-enabled `nats-server`
per test and returns a connection plus a cleanup function:

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
  Command, Event, Handler, Dead Letter, Outbox). The vocabulary above
  follows it throughout.
- [`docs/adr/`](docs/adr/) — the two decisions referenced above, recorded
  in full.

## License

[0BSD](LICENSE) — do whatever you want with it.
