package durablejob_test

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ludusrusso/nats-kit/durablejob"
	"github.com/ludusrusso/nats-kit/natstest"
)

func TestRunner_DispatchedJobIsDeliveredOnceAndAckedOnSuccess(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "ack", "steady"
	const ackWait = 500 * time.Millisecond

	rec := &recorder{}
	job := durablejob.NewJob(jobName, func(_ context.Context, payload []byte) error {
		rec.record(payload)
		return nil
	})

	runner := newRunner(t, nc, namespace, []durablejob.Job{job},
		durablejob.WithAckWait(ackWait), durablejob.WithMaxDeliver(3))
	stop := runInBackground(t, runner)
	defer stop()

	dispatch(t, d, namespace, jobName, "payload-1")

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 10*time.Second, "the dispatched job runs", func() bool {
		return rec.count() == 1
	})
	waitUntil(t, 10*time.Second, "the completed job is acked and nothing stays pending", func() bool {
		return consumerSettled(js, consumer)
	})

	// Two further AckWaits with no second Execute: a completed job is never
	// redelivered.
	staysAt(t, 2*ackWait, "a completed job is not redelivered", func() bool {
		return rec.count() == 1
	})

	if got := rec.snapshot(); len(got) != 1 || got[0] != "payload-1" {
		t.Fatalf("Execute saw %q, want the dispatched payload as-is", got)
	}
	if info := consumerInfo(t, js, consumer); info.NumRedelivered != 0 {
		t.Errorf("NumRedelivered = %d, want 0", info.NumRedelivered)
	}
	if _, found := dlqMessage(t, js, namespace, jobName); found {
		t.Errorf("a completed job must not be dead-lettered")
	}
}

func TestRunner_HeartbeatKeepsALongJobInsideOneDelivery(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "continue", "marathon"
	const ackWait = 500 * time.Millisecond
	const jobDuration = 2 * time.Second

	rec := &recorder{}
	job := durablejob.NewJob(jobName, func(ctx context.Context, payload []byte) error {
		rec.record(payload)
		select {
		case <-time.After(jobDuration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// The job legitimately needs four AckWaits: without the heartbeat the
	// delivery would expire while Execute runs and MaxDeliver 2 would
	// dead-letter a perfectly healthy job.
	runner := newRunner(t, nc, namespace, []durablejob.Job{job},
		durablejob.WithAckWait(ackWait), durablejob.WithMaxDeliver(2))
	stop := runInBackground(t, runner)
	defer stop()

	dispatch(t, d, namespace, jobName, "long-haul")

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 10*time.Second, "the long job starts", func() bool {
		return rec.count() == 1
	})
	waitUntil(t, 15*time.Second, "the long job completes inside one delivery", func() bool {
		return consumerSettled(js, consumer)
	})

	if got := rec.count(); got != 1 {
		t.Fatalf("Execute ran %d times, want 1: a job that is merely slow must stay inside one delivery", got)
	}
	if info := consumerInfo(t, js, consumer); info.NumRedelivered != 0 {
		t.Errorf("NumRedelivered = %d, want 0: heartbeats must prevent any redelivery while the job works", info.NumRedelivered)
	}
	if _, found := dlqMessage(t, js, namespace, jobName); found {
		t.Errorf("a job that is merely slow must never be dead-lettered")
	}
}

func TestRunner_FailingJobIsDeadLetteredAfterMaxDeliverGenuineFailures(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "poison", "boom"
	const ackWait = 500 * time.Millisecond
	const maxDeliver = 2

	rec := &recorder{}
	job := durablejob.NewJob(jobName, func(_ context.Context, payload []byte) error {
		rec.record(payload)
		return errors.New("boom")
	})

	runner := newRunner(t, nc, namespace, []durablejob.Job{job},
		durablejob.WithAckWait(ackWait), durablejob.WithMaxDeliver(maxDeliver))
	stop := runInBackground(t, runner)
	defer stop()

	dispatch(t, d, namespace, jobName, "doomed")

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 20*time.Second, "the exhausted job is dead-lettered", func() bool {
		_, found := dlqMessage(t, js, namespace, jobName)
		return found
	})

	dlq, _ := dlqMessage(t, js, namespace, jobName)
	if string(dlq.Data) != "doomed" {
		t.Errorf("DLQ payload = %q, want the original payload %q", dlq.Data, "doomed")
	}
	if got, want := dlq.Header.Get(durablejob.HeaderOriginSubject), durablejob.JobSubject(namespace, jobName); got != want {
		t.Errorf("%s = %q, want %q", durablejob.HeaderOriginSubject, got, want)
	}
	if got := dlq.Header.Get(durablejob.HeaderDeliveries); got != strconv.Itoa(maxDeliver) {
		t.Errorf("%s = %q, want %q", durablejob.HeaderDeliveries, got, strconv.Itoa(maxDeliver))
	}
	if seq, err := strconv.ParseUint(dlq.Header.Get(durablejob.HeaderOriginSequence), 10, 64); err != nil || seq == 0 {
		t.Errorf("%s = %q, want the original message's stream sequence", durablejob.HeaderOriginSequence, dlq.Header.Get(durablejob.HeaderOriginSequence))
	}
	if got := dlq.Header.Get(durablejob.HeaderError); !strings.Contains(got, "boom") {
		t.Errorf("%s = %q, want it to carry the failure", durablejob.HeaderError, got)
	}

	waitUntil(t, 10*time.Second, "the dead-lettered job is acked", func() bool {
		return consumerSettled(js, consumer)
	})
	staysAt(t, 2*ackWait, "a dead-lettered job is not redelivered", func() bool {
		return rec.count() == maxDeliver
	})
}

func TestRunner_PanickingJobIsContainedAndRetriedAsAGenuineFailure(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "contain", "panicky"
	const ackWait = 500 * time.Millisecond

	rec := &recorder{}
	job := durablejob.NewJob(jobName, func(_ context.Context, payload []byte) error {
		if rec.record(payload) == 1 {
			panic("kaboom")
		}
		return nil
	})

	runner := newRunner(t, nc, namespace, []durablejob.Job{job},
		durablejob.WithAckWait(ackWait), durablejob.WithMaxDeliver(3))
	stop := runInBackground(t, runner)
	defer stop()

	dispatch(t, d, namespace, jobName, "survive")

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 15*time.Second, "the retried job completes", func() bool {
		return consumerSettled(js, consumer) && rec.count() == 2
	})
	staysAt(t, 2*ackWait, "the completed job is not redelivered again", func() bool {
		return rec.count() == 2
	})

	if _, found := dlqMessage(t, js, namespace, jobName); found {
		t.Errorf("a panic that the retry recovers from must not be dead-lettered")
	}
}

func TestRunner_TwoInstancesOnTheSameJobGiveSingleDelivery(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "replicas", "shared"

	// Both instances record into the same recorder, so a job delivered twice
	// is visible as a payload seen twice.
	rec := &recorder{}
	newInstance := func() *durablejob.Runner {
		job := durablejob.NewJob(jobName, func(_ context.Context, payload []byte) error {
			rec.record(payload)
			return nil
		})
		return newRunner(t, nc, namespace, []durablejob.Job{job},
			durablejob.WithAckWait(5*time.Second), durablejob.WithMaxDeliver(3))
	}

	instanceA, instanceB := newInstance(), newInstance()
	stopA := runInBackground(t, instanceA)
	defer stopA()
	stopB := runInBackground(t, instanceB)
	defer stopB()

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 10*time.Second, "both instances bound the durable consumer", func() bool {
		return consumerExists(js, consumer)
	})

	want := []string{"job-1", "job-2", "job-3"}
	for _, payload := range want {
		dispatch(t, d, namespace, jobName, payload)
	}

	waitUntil(t, 15*time.Second, "every dispatched job is handled and acked", func() bool {
		return rec.count() == len(want) && consumerSettled(js, consumer)
	})
	staysAt(t, time.Second, "no job is delivered to both instances", func() bool {
		return rec.count() == len(want)
	})

	got := rec.snapshot()
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("handled payloads = %q, want %q exactly once each", got, want)
	}
}

func TestRunner_ShutdownMidExecuteNaksInsteadOfDeadLettering(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()
	js := newJetStream(t, nc)
	d := newDispatcher(t, nc)

	const namespace, jobName = "deploy", "rolling"
	// MaxDeliver 1 makes the point sharp: were shutdown treated as a verdict,
	// the very first delivery would already dead-letter the job.
	opts := []durablejob.Option{durablejob.WithAckWait(time.Second), durablejob.WithMaxDeliver(1)}

	stalled := &recorder{}
	stalling := durablejob.NewJob(jobName, func(ctx context.Context, payload []byte) error {
		stalled.record(payload)
		<-ctx.Done()
		return ctx.Err()
	})

	stopFirst := runInBackground(t, newRunner(t, nc, namespace, []durablejob.Job{stalling}, opts...))
	defer stopFirst()

	dispatch(t, d, namespace, jobName, "resume-me")
	waitUntil(t, 10*time.Second, "the first instance starts the job", func() bool {
		return stalled.count() == 1
	})
	stopFirst()

	if _, found := dlqMessage(t, js, namespace, jobName); found {
		t.Fatalf("a job interrupted by shutdown must not be dead-lettered")
	}

	resumed := &recorder{}
	resuming := durablejob.NewJob(jobName, func(_ context.Context, payload []byte) error {
		resumed.record(payload)
		return nil
	})
	stopSecond := runInBackground(t, newRunner(t, nc, namespace, []durablejob.Job{resuming}, opts...))
	defer stopSecond()

	consumer := consumerNameFor(namespace, jobName)
	waitUntil(t, 15*time.Second, "the redelivered job completes on the new instance", func() bool {
		return resumed.count() == 1 && consumerSettled(js, consumer)
	})

	if got := resumed.snapshot(); got[0] != "resume-me" {
		t.Errorf("the resumed job saw payload %q, want %q", got[0], "resume-me")
	}
	if _, found := dlqMessage(t, js, namespace, jobName); found {
		t.Errorf("the resumed job must not be dead-lettered")
	}
}
