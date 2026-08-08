package natstest_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-kit/natstest"
)

func TestStart_ReturnsAWorkingJetStreamEnabledConnection(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	if !nc.IsConnected() {
		t.Fatalf("connection returned by Start is not connected")
	}

	// A round trip through core NATS confirms the server actually accepts
	// traffic, not just that Connect returned without error. This alone
	// does not exercise JetStream at all: core NATS pub/sub works
	// identically whether or not the embedded server was started with
	// JetStream enabled, so this by itself proves nothing about Start's
	// specific promise.
	sub, err := nc.SubscribeSync("natstest.smoke")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	defer sub.Unsubscribe()

	if err := nc.Publish("natstest.smoke", []byte("ping")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if string(msg.Data) != "ping" {
		t.Fatalf("got %q, want %q", msg.Data, "ping")
	}

	// The actual claim in this test's name — a *JetStream-enabled*
	// connection — is only proven by a JetStream round trip: create a
	// stream, publish into it, and pull the message back out through a
	// consumer. Removing "JetStream: true" from Start's server.Options
	// makes CreateStream below fail immediately with "jetstream not
	// enabled" instead of silently degrading to core NATS, which is
	// exactly the regression this part of the test exists to catch.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "NATSTEST_JETSTREAM_SMOKE",
		Subjects: []string{"natstest.jetstream.smoke"},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	if _, err := js.Publish(ctx, "natstest.jetstream.smoke", []byte("jetstream-ping")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}

	jsMsg, err := cons.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(jsMsg.Data()) != "jetstream-ping" {
		t.Fatalf("got %q, want %q", jsMsg.Data(), "jetstream-ping")
	}
	if err := jsMsg.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestStart_TwoServersDoNotCollide(t *testing.T) {
	nc1, cleanup1 := natstest.Start(t)
	defer cleanup1()
	nc2, cleanup2 := natstest.Start(t)
	defer cleanup2()

	if nc1.ConnectedUrl() == nc2.ConnectedUrl() {
		t.Fatalf("two independently started servers ended up on the same address %q", nc1.ConnectedUrl())
	}
}
