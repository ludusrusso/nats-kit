package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs"
)

// Publisher publishes natscqrs.Records directly to NATS JetStream. It
// implements natscqrs.Sink, so a CommandBus or EventBus built on it sends
// straight to the commands/events streams — no Outbox involved.
//
// Call Close when a Publisher will no longer be used, typically as part of
// the same shutdown sequence that stops whatever calls Publish, to flush
// whatever publish is still in flight rather than abandoning it.
type Publisher struct {
	js     jetstream.JetStream
	logger *slog.Logger
}

// Compile-time check that Publisher satisfies natscqrs.Sink.
var _ natscqrs.Sink = (*Publisher)(nil)

// NewPublisher builds a Publisher on top of nc and ensures the streams it
// publishes into exist (see EnsureStreams — an existing stream is never
// reconfigured).
func NewPublisher(ctx context.Context, nc *nats.Conn, opts ...Option) (*Publisher, error) {
	if nc == nil {
		return nil, errors.New("natsjs: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("natsjs: build jetstream context: %w", err)
	}
	cfg := newConfigFrom(opts)
	if err := ensureStreams(ctx, js, cfg); err != nil {
		return nil, err
	}
	return &Publisher{js: js, logger: cfg.logger}, nil
}

// Publish sends every record to its Record.Subject, setting the
// Nats-Msg-Id header from Record.ID so JetStream deduplicates a repeat of
// it within its dedup window. That deduplication is what makes an Outbox's
// at-least-once delivery harmless downstream, so it is not optional: a
// Record published with an empty ID is still sent (Publish never drops a
// message on this account), but a warning is logged, since that Record can
// then be delivered more than once with no way for JetStream to notice.
//
// All records are published; if any fail, Publish still waits for the
// others and returns a single error describing every failure, each
// attributed to its subject.
//
// Publish sends every record with PublishMsgAsync and then waits for each
// one's ack, but only for as long as ctx allows: if ctx is done first,
// Publish reports that record as failed and moves on to the next one — it
// does not cancel the underlying async publish, which keeps running
// against the server regardless and may still succeed (or fail) after
// Publish has already returned an error for it. Nothing observes that
// outcome unless Close is called afterward.
//
// That gap is exactly the at-least-once case Nats-Msg-Id exists to cover.
// Picture Publisher sitting behind an Outbox forwarder: the forwarder
// calls Publish inside its own transaction-adjacent logic, Publish's ctx
// expires right as the ack was about to arrive, Publish reports failure,
// and the forwarder — reasonably, having been told the send failed — rolls
// back and will retry the same Record later. If the abandoned publish
// actually lands anyway, the retry produces a real duplicate on the wire;
// JetStream's deduplication window (see EnsureStreams, WithDuplicateWindow)
// is what turns that into a harmless one, discarding the second Nats-Msg-Id
// it already saw, exactly as ADR 0002 describes for the Outbox in general.
// Close does not close that gap — nothing but the dedup window does — it
// only gives a shutting-down process a way to wait for the outcome instead
// of walking away from every future mid-flight.
func (p *Publisher) Publish(ctx context.Context, records ...natscqrs.Record) error {
	if len(records) == 0 {
		return nil
	}

	type pending struct {
		subject string
		future  jetstream.PubAckFuture
	}
	pendings := make([]pending, 0, len(records))
	var failures []error

	for _, rec := range records {
		msg := &nats.Msg{Subject: rec.Subject, Data: rec.Data}
		var popts []jetstream.PublishOpt
		if rec.ID != "" {
			popts = append(popts, jetstream.WithMsgID(rec.ID))
		} else {
			p.logger.Warn("natsjs: publishing a record with no ID; JetStream cannot deduplicate a repeat of it", "subject", rec.Subject)
		}

		future, err := p.js.PublishMsgAsync(msg, popts...)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", rec.Subject, err))
			continue
		}
		pendings = append(pendings, pending{subject: rec.Subject, future: future})
	}

	for _, pd := range pendings {
		select {
		case <-pd.future.Ok():
		case err := <-pd.future.Err():
			failures = append(failures, fmt.Errorf("%s: %w", pd.subject, err))
		case <-ctx.Done():
			failures = append(failures, fmt.Errorf("%s: %w", pd.subject, ctx.Err()))
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("natsjs: publish: %d of %d record(s) failed: %w", len(failures), len(records), errors.Join(failures...))
	}
	return nil
}

// Close waits for every asynchronous publish still outstanding on this
// Publisher — including one Publish itself already gave up waiting on
// because its ctx ended first, see Publish's doc comment — to be
// acknowledged (or to fail) by the server, or for ctx to end first.
//
// Call Close once, when this Publisher will no longer be used: typically
// during the same shutdown sequence that stops whatever calls Publish, so
// a shutting-down process observes the true outcome of everything it sent
// instead of walking away from it mid-flight. Close returns nil once
// nothing is outstanding, or ctx.Err() if ctx ends first with publishes
// still pending — in which case those publishes are still running against
// the server and Close does not wait for them any further.
func (p *Publisher) Close(ctx context.Context) error {
	select {
	case <-p.js.PublishAsyncComplete():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
