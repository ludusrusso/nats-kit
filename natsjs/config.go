package natsjs

import (
	"fmt"
	"log/slog"
	"time"
)

// Default durations for the three streams EnsureStreams provisions. See the
// table in EnsureStreams's doc comment for what each one means.
//
// DefaultCommandsMaxAge is not an arbitrary tuning knob: Commands are
// delivered with DeliverAll, not DeliverNew (see runHandler's DeliverPolicy
// comment for why DeliverNew is not viable on a work-queue stream), so a
// newly created consumer can see whatever is already queued.
// DefaultCommandsMaxAge exists so that an orphaned Command — queued before
// a new consumer existed to receive it — expires instead of accumulating
// forever. See
// docs/adr/0001-commands-expire-and-new-consumers-start-from-now.md.
const (
	DefaultCommandsMaxAge   = 24 * time.Hour
	DefaultEventsMaxAge     = 7 * 24 * time.Hour
	DefaultDeadLetterMaxAge = 30 * 24 * time.Hour
)

// DefaultDuplicateWindow is the deduplication window EnsureStreams sets
// explicitly on every stream it provisions: how long JetStream remembers a
// Nats-Msg-Id well enough to reject an exact repeat of it. Its value (two
// minutes) happens to match the server's own built-in default, but it is
// set here explicitly rather than left implicit, because ADR 0002's entire
// mitigation for the Outbox's at-least-once delivery rests on this window:
// a forwarder (or any other retrying publisher) stalled longer than the
// window between a publish and its retry produces a genuine duplicate,
// since JetStream cannot deduplicate what it has already forgotten. See
// EnsureStreams's doc comment and WithDuplicateWindow.
const DefaultDuplicateWindow = 2 * time.Minute

// Defaults for the consumers Runner creates for each registered Handler.
const (
	// DefaultAckWait is how long JetStream waits for an Ack/Nak before
	// considering a delivery timed out and redelivering.
	DefaultAckWait = 30 * time.Second
	// DefaultMaxDeliver is how many times JetStream will attempt to
	// deliver a message to a Handler before it is eligible for the Dead
	// Letter.
	DefaultMaxDeliver = 4
	// DefaultWorkers is the number of messages a Handler processes
	// concurrently when no per-handler override is given: exactly one. A
	// silent default parallelism would break the assumption every Handler
	// author makes — that only one invocation of their function is ever
	// running at a time unless they asked for more.
	DefaultWorkers = 1
)

// config holds every setting an Option can change. It always starts from
// newConfig's defaults; an Option only ever narrows or overrides one field.
type config struct {
	logger *slog.Logger

	commandsMaxAge   time.Duration
	eventsMaxAge     time.Duration
	deadLetterMaxAge time.Duration
	duplicateWindow  time.Duration

	ackWait    time.Duration
	maxDeliver int

	// workers overrides DefaultWorkers on a per-handler-name basis. A
	// handler name absent from this map uses DefaultWorkers.
	workers map[string]int
}

func newConfig() *config {
	return &config{
		logger:           slog.Default(),
		commandsMaxAge:   DefaultCommandsMaxAge,
		eventsMaxAge:     DefaultEventsMaxAge,
		deadLetterMaxAge: DefaultDeadLetterMaxAge,
		duplicateWindow:  DefaultDuplicateWindow,
		ackWait:          DefaultAckWait,
		maxDeliver:       DefaultMaxDeliver,
		workers:          make(map[string]int),
	}
}

// workersFor returns the configured worker count for a Handler named name:
// DefaultWorkers unless WithHandlerWorkers overrode it.
func (c *config) workersFor(name string) int {
	if n, ok := c.workers[name]; ok {
		return n
	}
	return DefaultWorkers
}

func newConfigFrom(opts []Option) *config {
	cfg := newConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// validate rejects a config whose settings are not just unusual but
// nonsensical for a Runner to act on — the kind of value NATS itself would
// otherwise reject later, at Run, with a much less specific error. See
// New, which calls this.
func (c *config) validate() error {
	if c.maxDeliver < 1 {
		return fmt.Errorf(
			"natsjs: MaxDeliver must be at least 1 (got %d); to JetStream, "+
				"zero or a negative value means \"unlimited\" — the opposite "+
				"of what it means here, where it would instead make every "+
				"single delivery attempt satisfy \"attempts >= maxDeliver\" "+
				"and dead-letter a Handler's very first failure",
			c.maxDeliver,
		)
	}
	return nil
}

// Option configures stream provisioning (EnsureStreams, NewPublisher) and
// the Runner. Not every Option is meaningful to every one of those — e.g.
// WithHandlerWorkers only affects the Runner — unused settings are simply
// ignored by whichever of them doesn't need them.
type Option func(*config)

// WithLogger makes the transport log through logger instead of
// slog.Default(). See the package-level logging notes on Runner for what
// is logged and at what level.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithCommandsMaxAge overrides the cqrs-commands stream's MaxAge, applied
// only the first time EnsureStreams creates that stream. Changing this
// after the stream already exists has no effect: see EnsureStreams.
func WithCommandsMaxAge(d time.Duration) Option {
	return func(c *config) { c.commandsMaxAge = d }
}

// WithEventsMaxAge overrides the cqrs-events stream's MaxAge, applied only
// the first time EnsureStreams creates that stream.
func WithEventsMaxAge(d time.Duration) Option {
	return func(c *config) { c.eventsMaxAge = d }
}

// WithDeadLetterMaxAge overrides the cqrs-dlq stream's MaxAge, applied only
// the first time EnsureStreams creates that stream.
func WithDeadLetterMaxAge(d time.Duration) Option {
	return func(c *config) { c.deadLetterMaxAge = d }
}

// WithDuplicateWindow overrides the deduplication window (see
// DefaultDuplicateWindow) EnsureStreams sets on all three streams, applied
// only the first time EnsureStreams creates each one — exactly like
// WithCommandsMaxAge and friends. One setting applies to every stream: the
// dedup story (ADR 0002) is the same one wherever a Nats-Msg-Id travels.
func WithDuplicateWindow(d time.Duration) Option {
	return func(c *config) { c.duplicateWindow = d }
}

// WithAckWait overrides the AckWait every consumer the Runner creates uses:
// how long JetStream waits for an Ack/Nak before treating a delivery as
// timed out and redelivering it. This applies to every delivery attempt,
// including the first: this package deliberately has no escalating
// BackOff schedule (nats-server silently substitutes BackOff's first
// interval for AckWait whenever BackOff is non-empty, which would make
// this option a no-op — including for the very first attempt, not just
// "beyond" it — unless a caller also cleared BackOff; a flat AckWait
// applied to every attempt is simpler and does not have that trap).
func WithAckWait(d time.Duration) Option {
	return func(c *config) { c.ackWait = d }
}

// WithMaxDeliver overrides how many delivery attempts JetStream makes to a
// Handler before a message becomes eligible for the Dead Letter. n must be
// at least 1; New rejects anything less immediately, rather than letting
// NATS turn a zero or negative value into "unlimited attempts" and
// dead-letter a Handler's very first failure (see config.validate).
func WithMaxDeliver(n int) Option {
	return func(c *config) { c.maxDeliver = n }
}

// WithHandlerWorkers raises how many messages the Handler named
// handlerName processes concurrently, from DefaultWorkers (one) to
// workers. Pass a name that no registered Handler has and the option is
// simply never consulted.
//
// workers <= 0 is treated as DefaultWorkers: a Handler always processes at
// least one message at a time.
func WithHandlerWorkers(handlerName string, workers int) Option {
	return func(c *config) {
		if workers <= 0 {
			workers = DefaultWorkers
		}
		c.workers[handlerName] = workers
	}
}
