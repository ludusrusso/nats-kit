package cronjob_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ludusrusso/nats-cqrs/cronjob"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

func TestNew_RejectsANilConnection(t *testing.T) {
	if _, err := cronjob.New(context.Background(), nil, "ns", quiet()); err == nil {
		t.Fatal("New accepted a nil connection")
	}
}

func TestNew_RejectsAMalformedNamespace(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	for _, namespace := range []string{"", "   ", "with.dot", "with*star", "with>arrow", "with space", "with\ttab"} {
		t.Run(namespace, func(t *testing.T) {
			if _, err := cronjob.New(context.Background(), nc, namespace, quiet()); err == nil {
				t.Errorf("New accepted namespace %q", namespace)
			}
		})
	}
}

func TestRegister_RejectsWhatTheRunnerCannotHonour(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	noop := func(context.Context, cronjob.Tick) error { return nil }

	t.Run("a nil job", func(t *testing.T) {
		if err := newRunner(t, nc, "reg_nil").Register(nil); err == nil {
			t.Error("Register accepted a nil job")
		}
	})

	t.Run("a duplicate job name", func(t *testing.T) {
		r := newRunner(t, nc, "reg_dup")
		if err := r.Register(cronjob.NewJob("beat", "@every 1s", noop)); err != nil {
			t.Fatalf("Register: %v", err)
		}
		if err := r.Register(cronjob.NewJob("beat", "@every 5m", noop)); err == nil {
			t.Error("Register accepted a job name already registered on this Runner")
		}
	})

	t.Run("a malformed job name", func(t *testing.T) {
		for _, name := range []string{"", "   ", "with.dot", "with*star", "with>arrow", "with space"} {
			if err := newRunner(t, nc, "reg_name").Register(cronjob.NewJob(name, "@every 1s", noop)); err == nil {
				t.Errorf("Register accepted job name %q", name)
			}
		}
	})

	t.Run("an invalid schedule expression", func(t *testing.T) {
		err := newRunner(t, nc, "reg_schedule").Register(cronjob.NewJob("typo", "@evry 5m", noop))
		if err == nil {
			t.Fatal("Register accepted an invalid schedule expression")
		}
		if !strings.Contains(err.Error(), "typo") {
			t.Errorf("error %q does not name the offending job", err)
		}
	})

	t.Run("a job registered after Run has started", func(t *testing.T) {
		r := newRunner(t, nc, "reg_late")
		if err := r.Register(cronjob.NewJob("beat", "@every 1s", noop)); err != nil {
			t.Fatalf("Register: %v", err)
		}
		stop := runInBackground(t, r)
		defer stop()

		waitUntil(t, tickTimeout, "the runner has started", func() bool {
			return consumerExists(t, nc, "reg_late", "beat")
		})
		if err := r.Register(cronjob.NewJob("late", "@every 1s", noop)); err == nil {
			t.Error("Register accepted a job after Run had started")
		}
	})
}

func TestRun_RejectsAnEmptyRunnerAndASecondCall(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	noop := func(context.Context, cronjob.Tick) error { return nil }

	t.Run("no registered jobs", func(t *testing.T) {
		if err := newRunner(t, nc, "run_empty").Run(context.Background()); err == nil {
			t.Error("Run accepted a Runner with no registered jobs")
		}
	})

	t.Run("a second call", func(t *testing.T) {
		r := newRunner(t, nc, "run_twice")
		if err := r.Register(cronjob.NewJob("beat", "@every 1s", noop)); err != nil {
			t.Fatalf("Register: %v", err)
		}
		stop := runInBackground(t, r)
		defer stop()

		waitUntil(t, tickTimeout, "the runner has started", func() bool {
			return consumerExists(t, nc, "run_twice", "beat")
		})
		if err := r.Run(context.Background()); err == nil {
			t.Error("Run accepted a second call on the same Runner")
		}
	})
}
