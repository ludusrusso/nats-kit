package durablejob

import (
	"fmt"
	"log/slog"
	"time"
)

const (
	// DefaultAckWait is how long the server waits for an ack — or an
	// InProgress heartbeat — before redelivering a job.
	DefaultAckWait = 5 * time.Minute
	// DefaultMaxDeliver is how many genuine failures a job takes before it
	// is parked on the DLQ subject instead of being redelivered again.
	DefaultMaxDeliver = 5
)

// config holds every setting an Option can change.
type config struct {
	logger     *slog.Logger
	ackWait    time.Duration
	maxDeliver int
}

func newConfigFrom(opts []Option) *config {
	cfg := &config{
		logger:     slog.Default(),
		ackWait:    DefaultAckWait,
		maxDeliver: DefaultMaxDeliver,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// validate rejects settings that are not merely unusual but nonsensical, so
// New and NewDispatcher report them instead of NATS later.
func (c *config) validate() error {
	if c.ackWait <= 0 {
		return fmt.Errorf("durablejob: AckWait must be positive (got %s); the heartbeat ticks at half of it", c.ackWait)
	}
	if c.maxDeliver < 1 {
		return fmt.Errorf(
			"durablejob: MaxDeliver must be at least 1 (got %d); to JetStream, zero or a "+
				"negative value means \"unlimited\" — the opposite of what it means here, "+
				"where it would dead-letter a job's very first failure",
			c.maxDeliver,
		)
	}
	return nil
}

// Option configures a Runner or a Dispatcher.
type Option func(*config)

// WithLogger makes this package log through logger instead of slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithAckWait overrides how long the server waits for an ack or a heartbeat
// before redelivering a job (see DefaultAckWait).
func WithAckWait(d time.Duration) Option {
	return func(c *config) { c.ackWait = d }
}

// WithMaxDeliver overrides how many genuine failures a job takes before it is
// dead-lettered (see DefaultMaxDeliver).
func WithMaxDeliver(n int) Option {
	return func(c *config) { c.maxDeliver = n }
}
