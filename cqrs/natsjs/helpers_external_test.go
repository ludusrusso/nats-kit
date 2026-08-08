package natsjs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ludusrusso/nats-kit/cqrs"
	"github.com/ludusrusso/nats-kit/cqrs/natsjs"
)

// testCreateOrder and testOrderCreated are fixture Message types shared
// across this package's integration tests: a Command and an Event, each
// declared the only supported way — an embedded header with an explicit
// json:"header" tag. Every test gets its own embedded server (see
// natstest.Start), so reusing these two types across tests never causes
// cross-test interference.

type testCreateOrder struct {
	cqrs.CommandHeader `json:"header"`
	CustomerID         string `json:"customer_id"`
}

type testOrderCreated struct {
	cqrs.EventHeader `json:"header"`
	OrderID          string `json:"order_id"`
}

// collector is a minimal, concurrency-safe recorder of received messages,
// used by test Handlers to report what they saw back to the test body.
type collector[T any] struct {
	mu    sync.Mutex
	items []T
}

func (c *collector[T]) add(v T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = append(c.items, v)
}

func (c *collector[T]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *collector[T]) snapshot() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]T, len(c.items))
	copy(out, c.items)
	return out
}

// waitUntil polls cond every 10ms until it reports true, or fails the test
// after timeout. It is the sole synchronization primitive these tests use
// to observe asynchronous JetStream delivery, in preference to fixed
// sleeps.
func waitUntil(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// staysAt asserts that cond keeps reporting true for the whole of window —
// used to assert the *absence* of further delivery (e.g. a Handler that
// must not be invoked again after its message was dead-lettered, or a
// newly created DeliverNew consumer that must not receive history). window
// is kept short so tests stay fast; it is a best-effort check, not a proof.
func staysAt(t *testing.T, window time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("condition became false while it should have held: %s", msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// runInBackground starts r.Run in a goroutine and returns a stop function
// that cancels it and waits for it to return, failing the test if it
// doesn't shut down within a few seconds or returns anything other than
// ctx.Err() — Run's documented result for an ordinary, cancellation-driven
// shutdown (see Run's doc comment: it never returns nil on that path).
func runInBackground(t *testing.T, r *natsjs.Runner) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("runner.Run returned %v, want context.Canceled (ctx.Err()) for an ordinary shutdown", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("runner did not shut down within the timeout")
		}
	}
}
