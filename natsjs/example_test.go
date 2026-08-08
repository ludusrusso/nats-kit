package natsjs_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs"
	"github.com/ludusrusso/nats-cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
	"github.com/ludusrusso/nats-cqrs/outbox"
)

// PlaceOrder is a Command: an intent addressed to exactly one Handler.
//
// The `json:"header"` tag on the embedded CommandHeader is mandatory, not
// stylistic. Without it, Go promotes CommandHeader's ID/PublishedAt/Metadata
// fields inline into PlaceOrder's own JSON; a domain field whose tag then
// collides with "id", "published_at" or "metadata" would silently destroy
// the message's identity, with no error from encoding/json. This library
// detects a missing or wrong header tag and refuses to start rather than
// risk that.
type PlaceOrder struct {
	natscqrs.CommandHeader `json:"header"`
	CustomerID             string `json:"customer_id"`
}

// OrderPlaced is an Event: a fact, fanned out independently to every
// interested Handler. Same header rule as PlaceOrder above.
type OrderPlaced struct {
	natscqrs.EventHeader `json:"header"`
	OrderID              string `json:"order_id"`
	CustomerID           string `json:"customer_id"`
}

// Example wires one small service end to end: a Command Handler that does
// some work and records an Event, and an Event Handler that reacts to it.
//
// The Event goes out through an Outbox (outbox.Memory here, a real
// database-backed Reader in production) instead of straight to NATS, and a
// Forwarder drains it from there. The Command goes straight to NATS through
// a Publisher. Both paths end up on the exact same natscqrs.EventBus /
// natscqrs.CommandBus API: the domain code that calls Publish or Send never
// knows, or needs to know, which kind of Sink is underneath.
func Example() {
	// An Example has no *testing.T, so it uses StartServer rather than
	// Start. In a real test, natstest.Start(t) is the one you want.
	nc, cleanup, err := natstest.StartServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	// The Outbox. EventBus.Publish below writes here — in a real service,
	// inside the same database transaction as the work that produced the
	// Event — instead of to NATS directly.
	mem := outbox.NewMemory()
	events := natscqrs.NewEventBus(mem)

	pub, err := natsjs.NewPublisher(ctx, nc)
	if err != nil {
		panic(err)
	}
	commands := natscqrs.NewCommandBus(pub)

	handled := make(chan string, 1)
	received := make(chan OrderPlaced, 1)

	placeOrder := natscqrs.NewCommandHandler("place_order", func(ctx context.Context, cmd PlaceOrder) error {
		// The Handler's "work": a real service would touch its own
		// database here. This one just derives the fact that happened.
		evt := OrderPlaced{
			EventHeader: natscqrs.NewEventHeader(),
			OrderID:     "order-1",
			CustomerID:  cmd.CustomerID,
		}
		// This is the exact same call a Handler would make if events had
		// been built with NewEventBus(pub) instead of NewEventBus(mem) —
		// only the Sink swapped, from Outbox to direct NATS.
		if err := events.Publish(ctx, evt); err != nil {
			return err
		}
		handled <- cmd.CustomerID
		return nil
	})

	notifyCustomer := natscqrs.NewEventHandler("notify_customer", func(_ context.Context, evt OrderPlaced) error {
		received <- evt
		return nil
	})

	runner, err := natsjs.New(ctx, nc, "orders-svc")
	if err != nil {
		panic(err)
	}
	if err := runner.Register(placeOrder, notifyCustomer); err != nil {
		panic(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()

	// Events are delivered with DeliverNew (see ADR 0001 in docs/adr): a
	// Handler only ever sees what is published after its own durable
	// consumer exists. Waiting for it here — instead of, say, a fixed
	// sleep — is what makes forwarding the Outbox below safe to do next:
	// the Event Handler is guaranteed to be listening by the time anything
	// reaches NATS.
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	waitForDurableConsumer(js, natsjs.EventsStreamName, "orders-svc_notify_customer")

	if err := commands.Send(ctx, PlaceOrder{CustomerID: "cust-1"}); err != nil {
		panic(err)
	}
	customer := <-handled // blocks until place_order has run and published to the Outbox

	// Drain the Outbox into NATS. A long-running service does this by
	// running a Forwarder's Run loop in its own goroutine; a single Once
	// call here is that same drain, invoked by hand for a one-shot demo.
	forwarder := outbox.NewForwarder(mem, pub)
	if _, err := forwarder.Once(ctx); err != nil {
		panic(err)
	}

	evt := <-received // blocks until notify_customer has received the forwarded Event

	// Clean shutdown: cancel Run's context and wait for it to actually
	// stop before closing the Publisher underneath it.
	cancel()
	<-runErr
	if err := pub.Close(context.Background()); err != nil {
		panic(err)
	}

	fmt.Println("handled command for customer:", customer)
	fmt.Println("received event for order:", evt.OrderID, "customer:", evt.CustomerID)

	// Output:
	// handled command for customer: cust-1
	// received event for order: order-1 customer: cust-1
}

// waitForDurableConsumer blocks until durable exists on stream, or panics
// after 5s. See its call site above for why Example needs this.
func waitForDurableConsumer(js jetstream.JetStream, stream, durable string) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := js.Consumer(context.Background(), stream, durable); err == nil {
			return
		}
		if time.Now().After(deadline) {
			panic("natsjs example: durable consumer " + durable + " never appeared on stream " + stream)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
