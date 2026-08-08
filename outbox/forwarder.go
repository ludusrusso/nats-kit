package outbox

import (
	"context"
	"log/slog"
	"time"

	natscqrs "github.com/ludusrusso/nats-cqrs"
)

// Default configuration for a Forwarder built by NewForwarder; see
// WithBatchSize and WithPollInterval to override them.
const (
	defaultBatchSize    = 100
	defaultPollInterval = 500 * time.Millisecond
)

// Option configures a Forwarder built by NewForwarder.
type Option func(*Forwarder)

// WithBatchSize sets how many pending Records a single pass asks the
// Reader for. The default is 100. Values <= 0 are ignored.
func WithBatchSize(batch int) Option {
	return func(f *Forwarder) {
		if batch > 0 {
			f.batch = batch
		}
	}
}

// WithPollInterval sets how long Run waits before the next pass when the
// previous pass did not send a full batch. The default is 500ms. Values
// <= 0 are ignored. See Run's doc comment for when this interval is
// skipped entirely.
func WithPollInterval(d time.Duration) Option {
	return func(f *Forwarder) {
		if d > 0 {
			f.interval = d
		}
	}
}

// WithLogger sets the logger Run uses to report a failed pass. The default
// is slog.Default(). A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(f *Forwarder) {
		if l != nil {
			f.logger = l
		}
	}
}

// Forwarder is the loop that drains a Reader (an application's Outbox)
// into a natscqrs.Sink — typically a Sink that publishes straight to NATS.
// It is the "forwarder" the Outbox glossary entry and ADR 0002 refer to.
//
// Forwarder never starts itself. Nothing in this package spawns a
// goroutine, calls Run, or otherwise decides where or when a Forwarder
// runs: that is a deployment decision — a dedicated process, a goroutine
// inside a larger service, a sidecar, a cron-triggered job driving Once
// directly instead of Run — and it belongs entirely to the caller. This
// library only exposes the loop.
type Forwarder struct {
	reader   Reader
	sink     natscqrs.Sink
	batch    int
	interval time.Duration
	logger   *slog.Logger
}

// NewForwarder builds a Forwarder that drains r into sink, configured by
// opts. See WithBatchSize, WithPollInterval and WithLogger for the
// available options and their defaults.
func NewForwarder(r Reader, sink natscqrs.Sink, opts ...Option) *Forwarder {
	f := &Forwarder{
		reader:   r,
		sink:     sink,
		batch:    defaultBatchSize,
		interval: defaultPollInterval,
		logger:   slog.Default(),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Once drives a single pass: it asks the Reader to hand over up to the
// configured batch size of pending Records and publishes them through the
// Sink, as one ReadForSend call — so the send callback Reader.ReadForSend
// requires is simply f.sink.Publish. It returns how many Records were
// sent.
//
// Once is useful on its own, without Run: in tests, and for callers that
// want to drive the Outbox on their own schedule — a cron job, a queue
// worker's tick — instead of running Forwarder's own poll loop.
func (f *Forwarder) Once(ctx context.Context) (int, error) {
	return f.reader.ReadForSend(ctx, f.batch, func(ctx context.Context, records []natscqrs.Record) error {
		return f.sink.Publish(ctx, records...)
	})
}

// Run polls Once until ctx is cancelled. As with Forwarder itself, nothing
// calls Run automatically; where it runs is a deployment decision the
// caller makes, not this library.
//
// After a pass that sends a full batch (exactly the configured batch
// size), Run polls again immediately, without waiting: a backlog should
// drain at full speed, one full batch after another, not one poll interval
// at a time. After a pass that sends fewer Records than a full batch —
// including zero, when nothing was pending — Run waits the configured poll
// interval before the next pass.
//
// An error from a pass is logged (via the configured *slog.Logger) and
// never terminates Run: Run waits the poll interval and tries again. The
// only thing that ends Run is ctx being cancelled, in which case Run
// returns ctx.Err() — never nil, and never wrapped or discarded — as soon
// as the cancellation is observed, without logging that pass as a failure
// and without waiting out an interval. Cancellation is a clean shutdown
// signal, not an error condition to report.
func (f *Forwarder) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		sent, err := f.Once(ctx)

		if ctxErr := ctx.Err(); ctxErr != nil {
			// ctx was cancelled during (or right after) this pass: treat
			// this as the clean shutdown it is, not as a failed pass — do
			// not log err below, and do not wait out the poll interval.
			return ctxErr
		}

		if err != nil {
			f.logger.ErrorContext(ctx, "outbox: forwarder pass failed", "error", err)
		} else if sent >= f.batch {
			// A full batch: there may be more waiting right behind it.
			// Drain the backlog at full speed instead of idling.
			continue
		}

		timer := time.NewTimer(f.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
