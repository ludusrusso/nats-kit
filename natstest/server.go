// Package natstest starts a real, embedded, JetStream-enabled nats-server
// for use in tests.
//
// This project deliberately has no in-memory fake of the bus (see
// CONTEXT.md and the cqrs package's doc comment): the Publisher and the
// Runner are only meaningfully tested against a real NATS server, because so
// much of what they guarantee — work-queue competition, fan-out, redelivery,
// deduplication — is JetStream's behavior, not this library's. natstest
// exists so every package that needs that real server (cqrs/natsjs,
// cronjob and durablejob) starts one the same way.
package natstest

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// readyTimeout bounds how long the server is given to become ready for
// connections before starting it is treated as a failure.
const readyTimeout = 10 * time.Second

// StartServer starts an embedded nats-server with JetStream enabled, storing
// its state in a fresh temporary directory, and returns a connection to it
// along with a cleanup function that closes the connection, shuts the server
// down and removes that directory. Do not call cleanup more than once.
//
// The server listens on 127.0.0.1 on a randomly chosen free port, so
// concurrent servers never collide.
//
// Prefer Start in a test — it turns the error into t.Fatal, which is what a
// test wants. StartServer exists for the callers that have no testing.TB to
// hand: Example functions, and any standalone program that wants a throwaway
// JetStream server.
func StartServer() (*nats.Conn, func(), error) {
	storeDir, err := os.MkdirTemp("", "natstest-jetstream-*")
	if err != nil {
		return nil, nil, fmt.Errorf("natstest: failed to create a JetStream store directory: %w", err)
	}
	removeStore := func() { _ = os.RemoveAll(storeDir) }

	opts := &server.Options{
		Host:                   "127.0.0.1",
		Port:                   server.RANDOM_PORT,
		JetStream:              true,
		StoreDir:               storeDir,
		NoLog:                  true,
		NoSigs:                 true,
		DisableJetStreamBanner: true,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		removeStore()
		return nil, nil, fmt.Errorf("natstest: failed to build embedded nats-server: %w", err)
	}

	ns.Start()
	if !ns.ReadyForConnections(readyTimeout) {
		ns.Shutdown()
		removeStore()
		return nil, nil, fmt.Errorf("natstest: embedded nats-server did not become ready within %s", readyTimeout)
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		removeStore()
		return nil, nil, fmt.Errorf("natstest: failed to connect to embedded nats-server: %w", err)
	}

	cleanup := func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
		removeStore()
	}

	return nc, cleanup, nil
}

// Start is StartServer for a test: it starts the same embedded server and
// fails the test immediately (via t.Fatal) if that does not work, so callers
// can treat its return values as always valid and never check an error
// themselves.
//
// Call the returned cleanup function — typically via defer or t.Cleanup —
// when the test is done with the connection. Do not call it more than once.
func Start(t testing.TB) (*nats.Conn, func()) {
	t.Helper()

	nc, cleanup, err := StartServer()
	if err != nil {
		t.Fatal(err)
	}
	return nc, cleanup
}
