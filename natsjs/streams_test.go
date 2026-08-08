package natsjs_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/natsjs"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

func TestEnsureStreams_CreatesTheThreeStreamsWithExpectedDefaults(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	if err := natsjs.EnsureStreams(ctx, js); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	cases := []struct {
		name      string
		subject   string
		retention jetstream.RetentionPolicy
		maxAge    time.Duration
	}{
		{natsjs.CommandsStreamName, "commands.>", jetstream.WorkQueuePolicy, natsjs.DefaultCommandsMaxAge},
		{natsjs.EventsStreamName, "events.>", jetstream.LimitsPolicy, natsjs.DefaultEventsMaxAge},
		{natsjs.DeadLetterStreamName, "dlq.>", jetstream.LimitsPolicy, natsjs.DefaultDeadLetterMaxAge},
	}

	for _, tc := range cases {
		stream, err := js.Stream(ctx, tc.name)
		if err != nil {
			t.Fatalf("Stream(%q): %v", tc.name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Info(%q): %v", tc.name, err)
		}
		if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != tc.subject {
			t.Errorf("%s: Subjects = %v, want [%q]", tc.name, info.Config.Subjects, tc.subject)
		}
		if info.Config.Retention != tc.retention {
			t.Errorf("%s: Retention = %v, want %v", tc.name, info.Config.Retention, tc.retention)
		}
		if info.Config.MaxAge != tc.maxAge {
			t.Errorf("%s: MaxAge = %v, want %v", tc.name, info.Config.MaxAge, tc.maxAge)
		}
		if info.Config.Storage != jetstream.FileStorage {
			t.Errorf("%s: Storage = %v, want %v", tc.name, info.Config.Storage, jetstream.FileStorage)
		}
		if info.Config.Replicas != 1 {
			t.Errorf("%s: Replicas = %d, want 1", tc.name, info.Config.Replicas)
		}
		if info.Config.Duplicates != natsjs.DefaultDuplicateWindow {
			t.Errorf("%s: Duplicates = %v, want %v (the dedup window must be set explicitly, not left to the server's own default)",
				tc.name, info.Config.Duplicates, natsjs.DefaultDuplicateWindow)
		}
	}
}

func TestEnsureStreams_WithDuplicateWindowOverridesTheDedupWindowOnEveryStream(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const custom = 5 * time.Minute
	if err := natsjs.EnsureStreams(ctx, js, natsjs.WithDuplicateWindow(custom)); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	for _, name := range []string{natsjs.CommandsStreamName, natsjs.EventsStreamName, natsjs.DeadLetterStreamName} {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			t.Fatalf("Stream(%q): %v", name, err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatalf("Info(%q): %v", name, err)
		}
		if info.Config.Duplicates != custom {
			t.Errorf("%s: Duplicates = %v, want %v", name, info.Config.Duplicates, custom)
		}
	}
}

func TestEnsureStreams_DoesNotReconfigureAStreamThatAlreadyExists(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Simulate the stream already having been provisioned, by hand, with a
	// configuration that deliberately differs from this package's
	// defaults in every dimension EnsureStreams could otherwise touch.
	const customMaxAge = 90 * time.Minute
	preExisting := jetstream.StreamConfig{
		Name:      natsjs.CommandsStreamName,
		Subjects:  []string{"commands.>"},
		Retention: jetstream.LimitsPolicy, // not WorkQueuePolicy
		MaxAge:    customMaxAge,           // not DefaultCommandsMaxAge
		Storage:   jetstream.FileStorage,
		Replicas:  1,
	}
	if _, err := js.CreateStream(ctx, preExisting); err != nil {
		t.Fatalf("CreateStream (pre-existing): %v", err)
	}

	if err := natsjs.EnsureStreams(ctx, js, natsjs.WithCommandsMaxAge(24*time.Hour)); err != nil {
		t.Fatalf("EnsureStreams: %v", err)
	}

	stream, err := js.Stream(ctx, natsjs.CommandsStreamName)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	if info.Config.Retention != jetstream.LimitsPolicy {
		t.Errorf("Retention changed to %v; EnsureStreams must never reconfigure an existing stream", info.Config.Retention)
	}
	if info.Config.MaxAge != customMaxAge {
		t.Errorf("MaxAge changed to %v, want it to stay at the pre-existing %v", info.Config.MaxAge, customMaxAge)
	}
}
