package durablejob_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/durablejob"
)

// streamName and consumerNameFor spell out, from the outside, the names the
// package promises: the dedicated stream, and the durable every replica of a
// job binds.
const streamName = "durablejobs"

func consumerNameFor(namespace, job string) string { return "job_" + namespace + "_" + job }

// recorder is a concurrency-safe record of what a test job saw and did.
type recorder struct {
	mu       sync.Mutex
	calls    int
	payloads []string
}

func (r *recorder) record(payload []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.payloads = append(r.payloads, string(payload))
	return r.calls
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.payloads))
	copy(out, r.payloads)
	return out
}

// waitUntil polls cond every 10ms until it reports true, or fails the test
// after timeout. It is the sole synchronization primitive these tests use to
// observe asynchronous JetStream delivery, in preference to fixed sleeps.
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

// staysAt asserts cond keeps reporting true for the whole of window — used to
// assert the absence of a further delivery. window is kept short so tests stay
// fast; it is a best-effort check, not a proof.
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

func newJetStream(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// quiet keeps the suite's output to the test framework's own; it also covers
// WithLogger, which every test therefore exercises.
func quiet() durablejob.Option {
	return durablejob.WithLogger(slog.New(slog.DiscardHandler))
}

// newRunner builds a Runner with jobs registered on it.
func newRunner(t *testing.T, nc *nats.Conn, namespace string, jobs []durablejob.Job, opts ...durablejob.Option) *durablejob.Runner {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runner, err := durablejob.New(ctx, nc, namespace, append([]durablejob.Option{quiet()}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := runner.Register(jobs...); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return runner
}

// runInBackground starts r.Run in a goroutine and returns a stop function that
// cancels it and waits for it to return, failing the test if it does not shut
// down promptly or returns anything other than ctx.Err().
func runInBackground(t *testing.T, r *durablejob.Runner) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Run returned %v, want context.Canceled (ctx.Err()) for an ordinary shutdown", err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("runner did not shut down within the timeout")
			}
		})
	}
}

// newDispatcher builds a Dispatcher. Tests build it before starting a Runner:
// both constructors converge the stream, and nats-server 2.14.4 has a data
// race between a stream update and a concurrent consumer create.
func newDispatcher(t *testing.T, nc *nats.Conn) *durablejob.Dispatcher {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d, err := durablejob.NewDispatcher(ctx, nc, quiet())
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	return d
}

// dispatch starts one job.
func dispatch(t *testing.T, d *durablejob.Dispatcher, namespace, job, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.Dispatch(ctx, namespace, job, []byte(payload)); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
}

func consumerInfo(t *testing.T, js jetstream.JetStream, consumer string) *jetstream.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, streamName, consumer)
	if err != nil {
		t.Fatalf("Consumer %q: %v", consumer, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("Consumer %q info: %v", consumer, err)
	}
	return info
}

// consumerSettled reports whether the consumer exists with nothing delivered
// but unacked and nothing left to deliver — every dispatched job reached its
// final ack. Non-failing, so it can be polled before the consumer exists.
func consumerSettled(js jetstream.JetStream, consumer string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cons, err := js.Consumer(ctx, streamName, consumer)
	if err != nil {
		return false
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return false
	}
	return info.NumAckPending == 0 && info.NumPending == 0
}

func consumerExists(js jetstream.JetStream, consumer string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := js.Consumer(ctx, streamName, consumer)
	return err == nil
}

// dlqMessage reads the last dead-lettered message of a job kind; found is
// false when the DLQ subject is empty.
func dlqMessage(t *testing.T, js jetstream.JetStream, namespace, job string) (msg *jetstream.RawStreamMsg, found bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("Stream %q: %v", streamName, err)
	}
	msg, err = stream.GetLastMsgForSubject(ctx, durablejob.DLQSubject(namespace, job))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("GetLastMsgForSubject: %v", err)
	}
	return msg, true
}
