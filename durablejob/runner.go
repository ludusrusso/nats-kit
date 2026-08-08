package durablejob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Runner hosts registered Jobs and runs the redeliver-until-done loop for
// each of them against the durablejobs stream (ADR 0004).
type Runner struct {
	js        jetstream.JetStream
	namespace string
	cfg       *config

	mu      sync.Mutex
	started bool
	jobs    map[string]Job
}

// New builds a Runner for namespace on top of nc and provisions the
// durablejobs stream, so a provisioning failure is reported here rather than
// deferred to Run.
func New(ctx context.Context, nc *nats.Conn, namespace string, opts ...Option) (*Runner, error) {
	if nc == nil {
		return nil, errors.New("durablejob: nc must not be nil")
	}
	if err := validateName("namespace", namespace); err != nil {
		return nil, err
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
	return &Runner{
		js:        js,
		namespace: namespace,
		cfg:       cfg,
		jobs:      make(map[string]Job),
	}, nil
}

// Register adds jobs to the Runner, rejecting a nil job, an unusable name and
// a name already registered here; it must be called before Run.
func (r *Runner) Register(jobs ...Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("durablejob: cannot Register a job after Run has started")
	}

	for _, j := range jobs {
		if j == nil {
			return errors.New("durablejob: cannot register a nil job")
		}
		name := j.Name()
		if err := validateName("job name", name); err != nil {
			return err
		}
		if _, exists := r.jobs[name]; exists {
			return fmt.Errorf("durablejob: job %q is already registered", name)
		}
		r.jobs[name] = j
	}
	return nil
}

// Run creates a durable consumer for every registered Job and pulls for each
// of them until ctx is cancelled and every in-flight Execute has returned,
// returning ctx.Err() then — never nil on that path — or, immediately and
// non-nil, when Run was already called, when no Job is registered, or when
// some Job's consumer or iterator cannot be created.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("durablejob: Run has already been called on this Runner")
	}
	if len(r.jobs) == 0 {
		r.mu.Unlock()
		return errors.New("durablejob: Run called with no registered jobs; call Register before Run")
	}
	r.started = true
	jobs := make([]Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		jobs = append(jobs, j)
	}
	r.mu.Unlock()

	// runCtx lets the first failing job stop the others, so Run reports it
	// instead of waiting out the caller's ctx.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, len(jobs))
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.runJob(runCtx, j)
			errs[i] = err
			if err != nil {
				cancel()
			}
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return err
	}
	return ctx.Err()
}

// runJob binds j's durable consumer and handles one delivery at a time until
// ctx is cancelled and the in-flight Execute has returned.
func (r *Runner) runJob(ctx context.Context, j Job) error {
	name := j.Name()
	durable := consumerName(r.namespace, name)
	log := r.cfg.logger.With("namespace", r.namespace, "job", name)

	cons, err := r.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: JobSubject(r.namespace, name),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       r.cfg.ackWait,
		// One above the Runner's MaxDeliver: the extra delivery is the retry
		// window for the dead-letter hand-off itself (ADR 0004).
		MaxDeliver: r.cfg.maxDeliver + 1,
	})
	if err != nil {
		return fmt.Errorf("durablejob: create consumer %q on stream %q: %w", durable, streamName, err)
	}

	// One job at a time per instance; parallelism comes from replicas binding
	// the same durable (ADR 0004).
	it, err := cons.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		return fmt.Errorf("durablejob: start pulling messages for consumer %q: %w", durable, err)
	}

	log.Info("durablejob: consumer ready", "durable", durable, "subject", JobSubject(r.namespace, name))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg, err := it.Next()
			if err != nil {
				if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
					return
				}
				log.Warn("durablejob: failed to fetch next job message", "error", err)
				continue
			}
			r.handle(ctx, j, msg, log)
		}
	}()

	<-ctx.Done()

	it.Stop()
	<-done
	return nil
}

// handle runs one delivery: keep it alive while Execute works, ack exactly
// once on completion, and on a genuine failure choose between requeue and
// dead letter from the server-authoritative delivery count.
func (r *Runner) handle(ctx context.Context, j Job, msg jetstream.Msg, log *slog.Logger) {
	meta, err := msg.Metadata()
	if err != nil {
		// Without the server's delivery count the dead-letter decision would
		// be blind, so requeue and let the next delivery retry the read.
		log.Error("durablejob: cannot read job message metadata", "error", err)
		nak(msg, log)
		return
	}

	log = log.With("delivery", meta.NumDelivered)
	log.Info("durablejob: durable job delivery started")

	stopHeartbeat := r.startHeartbeat(msg, log)
	execErr := execute(ctx, j, msg.Data())
	stopHeartbeat()

	if execErr == nil {
		if err := msg.Ack(); err != nil {
			log.Warn("durablejob: failed to ack completed job", "error", err)
		}
		log.Info("durablejob: durable job completed")
		return
	}

	log.Error("durablejob: durable job execution failed", "error", execErr)

	if ctx.Err() != nil {
		// Shutdown is not a verdict on the job: requeue and skip the
		// dead-letter decision (ADR 0004).
		nak(msg, log)
		return
	}

	// NumDelivered is the server's own count, so exhaustion survives a Runner
	// restart (ADR 0004).
	if meta.NumDelivered >= uint64(r.cfg.maxDeliver) {
		if err := r.deadLetter(ctx, j, msg, meta, execErr); err != nil {
			// The job must not vanish: keep it pending so the backstop
			// delivery retries the hand-off.
			log.Error("durablejob: failed to dead-letter job", "error", err)
			nak(msg, log)
			return
		}
		log.Error("durablejob: durable job dead-lettered after exhausting deliveries")
		if err := msg.Ack(); err != nil {
			log.Warn("durablejob: failed to ack dead-lettered job", "error", err)
		}
		return
	}

	nak(msg, log)
}

// execute turns a panicking Job into an ordinary genuine failure, so one
// broken job cannot kill the process hosting the Runner.
func execute(ctx context.Context, j Job, payload []byte) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("durablejob: durable job panicked: %v", rec)
		}
	}()
	return j.Execute(ctx, payload)
}

// startHeartbeat extends the delivery's ack deadline at half the AckWait
// cadence while Execute runs; stop waits for the goroutine, so no heartbeat
// can race the final ack (ADR 0004).
func (r *Runner) startHeartbeat(msg jetstream.Msg, log *slog.Logger) (stop func()) {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(r.cfg.ackWait / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					log.Warn("durablejob: failed to extend job ack deadline", "error", err)
				}
			}
		}
	}()

	return func() {
		close(done)
		<-stopped
	}
}

// deadLetter republishes the exhausted job on the DLQ subject — still inside
// the durablejobs stream — with headers tracing it back to the original
// message.
func (r *Runner) deadLetter(ctx context.Context, j Job, msg jetstream.Msg, meta *jetstream.MsgMetadata, execErr error) error {
	dlq := &nats.Msg{
		Subject: DLQSubject(r.namespace, j.Name()),
		Data:    msg.Data(),
		Header:  nats.Header{},
	}
	dlq.Header.Set(HeaderOriginSubject, msg.Subject())
	dlq.Header.Set(HeaderOriginSequence, strconv.FormatUint(meta.Sequence.Stream, 10))
	dlq.Header.Set(HeaderDeliveries, strconv.FormatUint(meta.NumDelivered, 10))
	dlq.Header.Set(HeaderError, execErr.Error())

	_, err := r.js.PublishMsg(ctx, dlq)
	return err
}

func nak(msg jetstream.Msg, log *slog.Logger) {
	if err := msg.Nak(); err != nil {
		log.Warn("durablejob: failed to nak job message", "error", err)
	}
}
