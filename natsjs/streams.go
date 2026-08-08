package natsjs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// Stream names this package provisions and uses. Commands live in
// CommandsStreamName, Events in EventsStreamName, and every Handler's Dead
// Letters in DeadLetterStreamName.
const (
	CommandsStreamName   = "cqrs-commands"
	EventsStreamName     = "cqrs-events"
	DeadLetterStreamName = "cqrs-dlq"
)

// Subject filters bound to each stream.
const (
	commandsSubjects   = "commands.>"
	eventsSubjects     = "events.>"
	deadLetterSubjects = "dlq.>"
)

// DeadLetterSubjectPrefix is prepended to a Message's original subject to
// form the subject its Dead Letter is published on: "dlq." + subject.
const DeadLetterSubjectPrefix = "dlq."

// EnsureStreams creates the three streams this transport needs — Commands,
// Events, Dead Letters — if and only if they do not already exist:
//
//	stream          subjects    retention   default MaxAge   dedup window   storage
//	cqrs-commands   commands.>  work queue  24h              2m             file, 1 replica
//	cqrs-events     events.>    limits      7d               2m             file, 1 replica
//	cqrs-dlq        dlq.>       limits      30d              2m             file, 1 replica
//
// The MaxAge column is overridable via WithCommandsMaxAge, WithEventsMaxAge
// and WithDeadLetterMaxAge; the dedup window is overridable via
// WithDuplicateWindow, one setting applied to all three streams (see
// DefaultDuplicateWindow); everything else in the table is fixed.
//
// The dedup window is not a cosmetic default: it is set explicitly here,
// on every stream, rather than left to inherit the server's own built-in
// default (which happens to also be two minutes), because ADR 0002's
// entire mitigation for the Outbox's at-least-once delivery is JetStream
// deduplicating a repeat of a Record by its Nats-Msg-Id within this
// window. That bounds, but does not eliminate, the risk: a forwarder (or
// any other retrying publisher) stalled longer than the window between a
// publish and its retry produces a genuine duplicate, because JetStream
// has already forgotten the first attempt's Nats-Msg-Id by the time the
// retry arrives.
//
// A stream that already exists is left completely untouched, regardless of
// how its configuration compares to the one above. This is deliberate, not
// a shortcut: a stream is shared infrastructure that may be relied on by
// other services, other versions of this code, or hand-rolled tooling, and
// silently rewriting its retention policy, subjects or age limit out from
// under them is exactly the failure this function refuses to cause. If a
// stream's configuration genuinely needs to change, that is an explicit,
// reviewed operational decision — not something a client library does on
// every startup. EnsureStreams therefore only ever calls CreateStream, and
// only for a stream it first confirmed is missing; it never calls
// UpdateStream or CreateOrUpdateStream.
//
// Both Publisher and Runner call EnsureStreams themselves, so ordinary use
// needs no separate call. Call it directly when provisioning is its own
// step — e.g. a migration or deploy script that must succeed before any
// service using this package starts.
func EnsureStreams(ctx context.Context, js jetstream.JetStream, opts ...Option) error {
	cfg := newConfigFrom(opts)
	return ensureStreams(ctx, js, cfg)
}

func ensureStreams(ctx context.Context, js jetstream.JetStream, cfg *config) error {
	specs := []jetstream.StreamConfig{
		{
			Name:       CommandsStreamName,
			Subjects:   []string{commandsSubjects},
			Retention:  jetstream.WorkQueuePolicy,
			MaxAge:     cfg.commandsMaxAge,
			Duplicates: cfg.duplicateWindow,
			Storage:    jetstream.FileStorage,
			Replicas:   1,
		},
		{
			Name:       EventsStreamName,
			Subjects:   []string{eventsSubjects},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     cfg.eventsMaxAge,
			Duplicates: cfg.duplicateWindow,
			Storage:    jetstream.FileStorage,
			Replicas:   1,
		},
		{
			Name:       DeadLetterStreamName,
			Subjects:   []string{deadLetterSubjects},
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     cfg.deadLetterMaxAge,
			Duplicates: cfg.duplicateWindow,
			Storage:    jetstream.FileStorage,
			Replicas:   1,
		},
	}

	for _, sc := range specs {
		if err := ensureStream(ctx, js, sc, cfg.logger); err != nil {
			return fmt.Errorf("natsjs: ensure stream %q: %w", sc.Name, err)
		}
	}
	return nil
}

// ensureStream creates the stream described by sc if and only if no stream
// named sc.Name exists yet. See EnsureStreams's doc comment for why an
// existing stream is never touched, whatever its configuration.
func ensureStream(ctx context.Context, js jetstream.JetStream, sc jetstream.StreamConfig, logger *slog.Logger) error {
	_, err := js.Stream(ctx, sc.Name)
	if err == nil {
		logger.Debug("natsjs: stream already exists, leaving its configuration untouched", "stream", sc.Name)
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("natsjs: look up stream %q: %w", sc.Name, err)
	}

	if _, err := js.CreateStream(ctx, sc); err != nil {
		// The stream may have just been created by a concurrent caller
		// (another process, or this package's own Publisher and Runner
		// racing at startup) between our lookup and this call. That is not
		// a failure: the stream now exists either way, and we still never
		// reconfigured one that already existed at the time it mattered.
		if errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			logger.Debug("natsjs: stream was created concurrently, leaving its configuration untouched", "stream", sc.Name)
			return nil
		}
		return fmt.Errorf("natsjs: create stream %q: %w", sc.Name, err)
	}

	logger.Info("natsjs: created stream", "stream", sc.Name, "subjects", sc.Subjects, "max_age", sc.MaxAge)
	return nil
}
