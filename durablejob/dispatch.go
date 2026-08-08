package durablejob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Dispatcher starts Durable Jobs, including jobs owned by another service.
type Dispatcher struct {
	js     jetstream.JetStream
	logger *slog.Logger
}

// NewDispatcher builds a Dispatcher on top of nc and provisions the
// durablejobs stream, so a dispatching service does not depend on a Runner
// having started first.
func NewDispatcher(ctx context.Context, nc *nats.Conn, opts ...Option) (*Dispatcher, error) {
	if nc == nil {
		return nil, errors.New("durablejob: nc must not be nil")
	}
	cfg := newConfigFrom(opts)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("durablejob: build jetstream context: %w", err)
	}
	if err := ensureStream(ctx, js); err != nil {
		return nil, err
	}
	return &Dispatcher{js: js, logger: cfg.logger}, nil
}

// Dispatch starts one Durable Job by publishing one message on the job
// subject; payload identifies the work and never carries progress (ADR 0004).
func (d *Dispatcher) Dispatch(ctx context.Context, namespace, job string, payload []byte) error {
	if err := validateName("namespace", namespace); err != nil {
		return err
	}
	if err := validateName("job name", job); err != nil {
		return err
	}

	if _, err := d.js.Publish(ctx, JobSubject(namespace, job), payload); err != nil {
		return fmt.Errorf("durablejob: dispatch durable job %s/%s: %w", namespace, job, err)
	}
	d.logger.Debug("durablejob: durable job dispatched", "namespace", namespace, "job", job)
	return nil
}
