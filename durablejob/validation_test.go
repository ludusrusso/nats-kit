package durablejob_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ludusrusso/nats-cqrs/durablejob"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

// badNames are the tokens that would corrupt the dot-separated subject space
// or the underscore-joined consumer name.
var badNames = []string{"", "   ", "bad.ns", "bad job", "wild*", "wild>", "tab\tname"}

func noop(context.Context, []byte) error { return nil }

func TestNew_RejectsANilConnection(t *testing.T) {
	if _, err := durablejob.New(context.Background(), nil, "ns"); err == nil {
		t.Fatal("New with a nil connection: want an error, got nil")
	}
}

func TestNew_RejectsAnInvalidNamespace(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	for _, name := range badNames {
		if _, err := durablejob.New(context.Background(), nc, name); err == nil {
			t.Errorf("New with namespace %q: want an error, got nil", name)
		}
	}
}

func TestNew_RejectsNonsensicalOptions(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	if _, err := durablejob.New(context.Background(), nc, "ns", durablejob.WithMaxDeliver(0)); err == nil {
		t.Error("New with MaxDeliver 0: want an error, got nil")
	}
	if _, err := durablejob.New(context.Background(), nc, "ns", durablejob.WithAckWait(0)); err == nil {
		t.Error("New with AckWait 0: want an error, got nil")
	}
}

func TestRegister_RejectsNilInvalidAndDuplicateJobs(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	runner, err := durablejob.New(context.Background(), nc, "ns")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := runner.Register(nil); err == nil {
		t.Error("Register(nil): want an error, got nil")
	}
	for _, name := range badNames {
		if err := runner.Register(durablejob.NewJob(name, noop)); err == nil {
			t.Errorf("Register with job name %q: want an error, got nil", name)
		}
	}
	if err := runner.Register(durablejob.NewJob("rebuild", noop)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := runner.Register(durablejob.NewJob("rebuild", noop)); err == nil {
		t.Error("Register with an already registered name: want an error, got nil")
	}
}

func TestRegister_RejectsRegistrationAfterRunHasStarted(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)

	runner := newRunner(t, nc, "late", []durablejob.Job{durablejob.NewJob("first", noop)})
	stop := runInBackground(t, runner)
	defer stop()

	waitUntil(t, 10*time.Second, "the runner has started", func() bool {
		return consumerExists(js, consumerNameFor("late", "first"))
	})

	if err := runner.Register(durablejob.NewJob("second", noop)); err == nil {
		t.Error("Register after Run has started: want an error, got nil")
	}
}

func TestRun_RejectsAnEmptyRunnerAndASecondCall(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	empty, err := durablejob.New(context.Background(), nc, "empty")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := empty.Run(context.Background()); err == nil {
		t.Error("Run with no registered jobs: want an error, got nil")
	}

	runner := newRunner(t, nc, "twice", []durablejob.Job{durablejob.NewJob("only", noop)})
	stop := runInBackground(t, runner)
	defer stop()

	waitUntil(t, 10*time.Second, "the first Run has started", func() bool {
		return consumerExists(newJetStream(t, nc), consumerNameFor("twice", "only"))
	})

	if err := runner.Run(context.Background()); err == nil {
		t.Error("a second Run on the same Runner: want an error, got nil")
	}
}

func TestDispatch_RejectsInvalidNames(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	d, err := durablejob.NewDispatcher(context.Background(), nc)
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	for _, name := range badNames {
		if err := d.Dispatch(context.Background(), name, "rebuild", nil); err == nil {
			t.Errorf("Dispatch with namespace %q: want an error, got nil", name)
		}
		err := d.Dispatch(context.Background(), "ns", name, nil)
		if err == nil {
			t.Errorf("Dispatch with job name %q: want an error, got nil", name)
			continue
		}
		if !strings.Contains(err.Error(), "job name") {
			t.Errorf("Dispatch with job name %q reported %v, want it to name the offending token", name, err)
		}
	}
}

func TestNewDispatcher_RejectsANilConnection(t *testing.T) {
	if _, err := durablejob.NewDispatcher(context.Background(), nil); err == nil {
		t.Fatal("NewDispatcher with a nil connection: want an error, got nil")
	}
}
