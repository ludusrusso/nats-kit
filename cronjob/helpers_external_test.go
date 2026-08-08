package cronjob_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/cronjob"
)

// waitUntil polls cond every 10ms until it reports true, or fails the test
// after timeout. It is the sole synchronization primitive these tests use to
// observe server-generated Ticks, in preference to fixed sleeps.
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

// staysAt asserts that cond keeps reporting true for the whole of window,
// used to assert the absence of further state change. window is kept short so
// tests stay fast; it is a best-effort check, not a proof.
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

// tickRecord is one Tick as seen by one bound instance.
type tickRecord struct {
	instance int
	tick     cronjob.Tick
}

// recorder collects the Ticks delivered across every instance of a test.
type recorder struct {
	mu      sync.Mutex
	records []tickRecord
}

// handler builds a Handler that records every Tick it receives as coming from
// instance.
func (r *recorder) handler(instance int) cronjob.Handler {
	return func(_ context.Context, tick cronjob.Tick) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.records = append(r.records, tickRecord{instance: instance, tick: tick})
		return nil
	}
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func (r *recorder) snapshot() []tickRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tickRecord, len(r.records))
	copy(out, r.records)
	return out
}

// probe is a Handler that counts its invocations and the peak number of them
// running at once across every bound instance.
type probe struct {
	mu        sync.Mutex
	active    int
	maxActive int
	calls     int
}

// handler builds a Handler that holds a Tick open for work and then returns
// err.
func (p *probe) handler(work time.Duration, err error) cronjob.Handler {
	return func(_ context.Context, _ cronjob.Tick) error {
		p.mu.Lock()
		p.active++
		p.calls++
		if p.active > p.maxActive {
			p.maxActive = p.active
		}
		p.mu.Unlock()

		time.Sleep(work)

		p.mu.Lock()
		p.active--
		p.mu.Unlock()
		return err
	}
}

func (p *probe) peak() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxActive
}

func (p *probe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// loggedRecord is one record a Runner emitted, flattened to what these tests
// assert on.
type loggedRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// logRecorder is a slog.Handler that keeps every record handed to it, so a
// test can assert what the Runner logged.
type logRecorder struct {
	mu      sync.Mutex
	records []loggedRecord
}

func (h *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	attrs := map[string]string{}
	rec.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, loggedRecord{level: rec.Level, msg: rec.Message, attrs: attrs})
	return nil
}

func (h *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logRecorder) WithGroup(string) slog.Handler      { return h }

// has reports whether a record of level with exactly msg carries attr key=want.
func (h *logRecorder) has(level slog.Level, msg, key, want string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rec := range h.records {
		if rec.level == level && rec.msg == msg && rec.attrs[key] == want {
			return true
		}
	}
	return false
}

// quiet keeps a test's own expected failures out of the test binary's output.
func quiet() cronjob.Option { return cronjob.WithLogger(slog.New(slog.DiscardHandler)) }

// newRunner builds a Runner for namespace, failing the test if it cannot.
func newRunner(t *testing.T, nc *nats.Conn, namespace string, opts ...cronjob.Option) *cronjob.Runner {
	t.Helper()
	r, err := cronjob.New(context.Background(), nc, namespace, append([]cronjob.Option{quiet()}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// runInBackground starts r.Run in a goroutine and returns a stop function that
// cancels it and waits for it to return, failing the test if it does not shut
// down promptly or returns anything other than ctx.Err().
func runInBackground(t *testing.T, r *cronjob.Runner) (stop func()) {
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
					t.Errorf("runner.Run returned %v, want context.Canceled (ctx.Err()) for an ordinary shutdown", err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("runner did not shut down within the timeout")
			}
		})
	}
}

// startRunner builds a Runner for namespace, registers jobs on it and runs it
// in the background.
func startRunner(t *testing.T, nc *nats.Conn, namespace string, jobs ...cronjob.Job) (stop func()) {
	t.Helper()
	r := newRunner(t, nc, namespace)
	if err := r.Register(jobs...); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return runInBackground(t, r)
}

// newInstances builds count Runners bound to namespace, each registering the
// job jobFor declares for it. Every Runner is built before any of them is
// started because New provisions the stream, and nats-server 2.14.4 races a
// stream update against a concurrently created consumer.
func newInstances(t *testing.T, nc *nats.Conn, namespace string, count int, jobFor func(instance int) cronjob.Job) []*cronjob.Runner {
	t.Helper()
	runners := make([]*cronjob.Runner, count)
	for i := range runners {
		runners[i] = newRunner(t, nc, namespace)
		if err := runners[i].Register(jobFor(i)); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	return runners
}

// stream returns a handle on the cronjobs stream these tests inspect directly.
func stream(t *testing.T, nc *nats.Conn) jetstream.Stream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	s, err := js.Stream(context.Background(), "cronjobs")
	if err != nil {
		t.Fatalf("stream cronjobs: %v", err)
	}
	return s
}

// scheduleDef reads the stored Schedule definition of a job, reporting false
// when no definition is stored for it.
func scheduleDef(t *testing.T, nc *nats.Conn, namespace, job string) (*jetstream.RawStreamMsg, bool) {
	t.Helper()
	msg, err := stream(t, nc).GetLastMsgForSubject(context.Background(), "cronjobs.schedules."+namespace+"."+job)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("GetLastMsgForSubject: %v", err)
	}
	return msg, true
}

func scheduleExists(t *testing.T, nc *nats.Conn, namespace, job string) bool {
	t.Helper()
	_, ok := scheduleDef(t, nc, namespace, job)
	return ok
}

func consumerExists(t *testing.T, nc *nats.Conn, namespace, job string) bool {
	t.Helper()
	_, err := stream(t, nc).Consumer(context.Background(), "cron_"+namespace+"_"+job)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	return true
}
