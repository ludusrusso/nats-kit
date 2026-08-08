package cronjob_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/cronjob"
	"github.com/ludusrusso/nats-cqrs/natstest"
)

// tickTimeout bounds how long a test waits for server-generated Ticks of a
// job scheduled every second.
const tickTimeout = 15 * time.Second

func TestRunner_TickCarriesScheduledAtAndNextAt(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	rec := &recorder{}
	stop := startRunner(t, nc, "timing", cronjob.NewJob("beat", "@every 1s", rec.handler(0)))
	defer stop()

	waitUntil(t, tickTimeout, "the first tick is delivered", func() bool { return rec.len() >= 1 })

	first := rec.snapshot()[0].tick
	if first.ScheduledAt.IsZero() {
		t.Error("Tick.ScheduledAt is zero, want the moment the execution was due")
	}
	if first.NextAt.IsZero() {
		t.Fatal("Tick.NextAt is zero, want the moment the next Tick fires")
	}
	if delta := first.NextAt.Sub(first.ScheduledAt); delta < 500*time.Millisecond || delta > 1500*time.Millisecond {
		t.Errorf("NextAt is %s after ScheduledAt, want ~1s (the declared interval)", delta)
	}
}

func TestRunner_StreamSubjectAndConsumerNaming(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	rec := &recorder{}
	stop := startRunner(t, nc, "naming", cronjob.NewJob("beat", "@every 1s", rec.handler(0)))
	defer stop()

	waitUntil(t, tickTimeout, "the first tick is delivered", func() bool { return rec.len() >= 1 })

	def, ok := scheduleDef(t, nc, "naming", "beat")
	if !ok {
		t.Fatal("no schedule definition stored on cronjobs.schedules.naming.beat")
	}
	if got := def.Header.Get(jetstream.ScheduleHeader); got != "@every 1s" {
		t.Errorf("stored schedule expression is %q, want %q", got, "@every 1s")
	}
	if got := def.Header.Get(jetstream.ScheduleTargetHeader); got != "cronjobs.ticks.naming.beat" {
		t.Errorf("schedule target is %q, want %q", got, "cronjobs.ticks.naming.beat")
	}
	if got := def.Header.Get(jetstream.ScheduleTTLHeader); got != (5 * time.Second).String() {
		t.Errorf("tick TTL is %q, want %q (5× the declared interval)", got, (5 * time.Second).String())
	}
	if !consumerExists(t, nc, "naming", "beat") {
		t.Error("durable consumer cron_naming_beat does not exist")
	}
}

func TestRunner_EachTickIsHandledByExactlyOneBoundInstance(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	const namespace, jobName = "delivery", "beat"
	rec := &recorder{}
	runners := newInstances(t, nc, namespace, 3, func(i int) cronjob.Job {
		return cronjob.NewJob(jobName, "@every 1s", rec.handler(i))
	})

	// The first instance is given its first Tick before the others join, so
	// they find the Schedule already published and this test measures single
	// delivery rather than three instances publishing the same Schedule at
	// once.
	stop := runInBackground(t, runners[0])
	defer stop()
	waitUntil(t, tickTimeout, "the first instance receives a tick", func() bool { return rec.len() >= 1 })

	for _, r := range runners[1:] {
		stopOther := runInBackground(t, r)
		defer stopOther()
	}

	waitUntil(t, tickTimeout, "several ticks are delivered across the bound instances", func() bool {
		return rec.len() >= 4
	})

	seen := map[time.Time]int{}
	for _, r := range rec.snapshot() {
		seen[r.tick.ScheduledAt]++
	}
	for scheduledAt, count := range seen {
		if count != 1 {
			t.Errorf("the Tick scheduled at %s was handled %d times, want exactly once", scheduledAt, count)
		}
	}
}

func TestRunner_JobNeverOverlapsItselfAcrossBoundInstances(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	const namespace, jobName = "overlap", "slow"

	// The handler runs longer than the interval: without MaxAckPending: 1, a
	// Tick generated while one instance is busy would go to another instance.
	p := &probe{}
	runners := newInstances(t, nc, namespace, 3, func(int) cronjob.Job {
		return cronjob.NewJob(jobName, "@every 1s", p.handler(2*time.Second, nil))
	})

	stop := runInBackground(t, runners[0])
	defer stop()
	waitUntil(t, tickTimeout, "the first instance is running the job", func() bool { return p.count() >= 1 })

	for _, r := range runners[1:] {
		stopOther := runInBackground(t, r)
		defer stopOther()
	}

	waitUntil(t, 25*time.Second, "the slow job fires at least twice", func() bool { return p.count() >= 2 })

	if peak := p.peak(); peak != 1 {
		t.Errorf("%d invocations of the job ran at once, want 1: MaxAckPending: 1 non-overlap violated", peak)
	}
}

func TestRunner_FailingHandlerIsLoggedAckedAndKeepsTicking(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	const namespace, jobName = "errors", "boom"

	logs := &logRecorder{}
	p := &probe{}
	r := newRunner(t, nc, namespace, cronjob.WithLogger(slog.New(logs)))
	if err := r.Register(cronjob.NewJob(jobName, "@every 1s", p.handler(0, errors.New("boom")))); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	waitUntil(t, 25*time.Second, "the failing job keeps ticking", func() bool { return p.count() >= 3 })

	// The failing Tick is acked, so the next one arrives on the job's own
	// cadence: a redelivery storm would push the count far past the seconds
	// elapsed.
	if calls := p.count(); calls > 12 {
		t.Errorf("the handler ran %d times in ~3s of ticking, want no redelivery of a failed Tick", calls)
	}

	if !logs.has(slog.LevelError, "cronjob: cron job handler failed", "job", jobName) {
		t.Error("the handler failure was not logged at Error level with the job's identity")
	}

	select {
	case err := <-runErr:
		t.Fatalf("Run returned %v on a handler error, want it still running", err)
	default:
	}

	cancel()
	if err := <-runErr; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
}

func TestRunner_RepublishesAChangedScheduleOnly(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	const namespace, jobName = "convergence", "beat"

	seq := func() uint64 {
		def, ok := scheduleDef(t, nc, namespace, jobName)
		if !ok {
			t.Fatal("no schedule definition stored")
		}
		return def.Sequence
	}

	rec := &recorder{}
	stop := startRunner(t, nc, namespace, cronjob.NewJob(jobName, "@every 1s", rec.handler(0)))
	waitUntil(t, tickTimeout, "the first instance receives a tick", func() bool { return rec.len() >= 1 })
	first := seq()
	stop()

	// A restart declaring the same expression must not republish: the stored
	// definition keeps its sequence, so the running cadence keeps its phase.
	restarted := &recorder{}
	stop = startRunner(t, nc, namespace, cronjob.NewJob(jobName, "@every 1s", restarted.handler(0)))
	waitUntil(t, tickTimeout, "the restarted instance receives a tick", func() bool { return restarted.len() >= 1 })
	staysAt(t, 300*time.Millisecond, "an unchanged expression is not republished", func() bool {
		return seq() == first
	})
	stop()

	// A changed expression is republished, with a Tick TTL rescaled to 5× the
	// new interval.
	changed := &recorder{}
	stop = startRunner(t, nc, namespace, cronjob.NewJob(jobName, "@every 2s", changed.handler(0)))
	defer stop()

	waitUntil(t, tickTimeout, "the changed expression is republished", func() bool {
		def, ok := scheduleDef(t, nc, namespace, jobName)
		return ok && def.Sequence > first && def.Header.Get(jetstream.ScheduleHeader) == "@every 2s"
	})
	def, _ := scheduleDef(t, nc, namespace, jobName)
	if got := def.Header.Get(jetstream.ScheduleTTLHeader); got != (10 * time.Second).String() {
		t.Errorf("tick TTL is %q, want %q (5× the new interval)", got, (10 * time.Second).String())
	}
}

func TestRunner_ReconcilesOrphansOfItsOwnNamespaceOnly(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	const namespace, other = "reconcile", "neighbour"

	// A neighbouring namespace, provisioned by its own now-stopped process:
	// its state must survive another namespace's reconciliation untouched.
	neighbour := &recorder{}
	stopOther := startRunner(t, nc, other, cronjob.NewJob("keep", "@every 1s", neighbour.handler(0)))
	waitUntil(t, tickTimeout, "the neighbouring job is provisioned", func() bool { return neighbour.len() >= 1 })
	stopOther()

	rec := &recorder{}
	stop := startRunner(t, nc, namespace,
		cronjob.NewJob("stay", "@every 1s", rec.handler(0)),
		cronjob.NewJob("gone", "@every 1s", rec.handler(1)),
	)
	waitUntil(t, tickTimeout, "both jobs are provisioned", func() bool {
		return scheduleExists(t, nc, namespace, "stay") && scheduleExists(t, nc, namespace, "gone") &&
			consumerExists(t, nc, namespace, "stay") && consumerExists(t, nc, namespace, "gone")
	})
	stop()

	// The second startup no longer declares "gone".
	restarted := &recorder{}
	stop = startRunner(t, nc, namespace, cronjob.NewJob("stay", "@every 1s", restarted.handler(0)))
	defer stop()

	waitUntil(t, tickTimeout, "the orphan job is reconciled away", func() bool {
		return !scheduleExists(t, nc, namespace, "gone") && !consumerExists(t, nc, namespace, "gone")
	})
	if !scheduleExists(t, nc, namespace, "stay") || !consumerExists(t, nc, namespace, "stay") {
		t.Error("the still-declared job lost its schedule or its consumer")
	}
	if !scheduleExists(t, nc, other, "keep") || !consumerExists(t, nc, other, "keep") {
		t.Error("reconciliation reached into another namespace's schedules or consumers")
	}
}
