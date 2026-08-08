package cronjob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// streamName is the stream carrying every Schedule definition and every
	// Tick this package produces.
	streamName = "cronjobs"

	schedulesSubjectPrefix = streamName + ".schedules"
	ticksSubjectPrefix     = streamName + ".ticks"

	// ackWait is the fixed acknowledgement window of every Tick consumer,
	// deliberately not per-job configuration (ADR 0003).
	ackWait = 5 * time.Minute

	// tickTTLMultiplier sizes a Tick's server-side lifetime at 5× its job's
	// interval, bounding the catch-up burst after an outage (ADR 0003).
	tickTTLMultiplier = 5

	// schedulePurgeSentinel is what the server writes into Nats-Schedule-Next
	// for a Schedule that will not fire again.
	schedulePurgeSentinel = "purge"
)

// Runner converges the Cron Jobs registered on it onto NATS JetStream and
// dispatches each Tick to exactly one of the instances bound to that job.
type Runner struct {
	js        jetstream.JetStream
	stream    jetstream.Stream
	namespace string
	cfg       *config

	mu      sync.Mutex
	started bool
	jobs    map[string]Job
}

// New builds a Runner owning namespace on top of nc and provisions the
// cronjobs stream up front, so a provisioning failure is reported here rather
// than deferred to Run.
//
// namespace becomes a token of every subject and consumer name this Runner
// creates: it must be non-empty and free of '.', '*', '>' and whitespace. One
// Runner owns exactly one namespace, and reconciliation never reaches outside
// it (ADR 0003).
func New(ctx context.Context, nc *nats.Conn, namespace string, opts ...Option) (*Runner, error) {
	if nc == nil {
		return nil, errors.New("cronjob: nc must not be nil")
	}
	if err := validateName("namespace", namespace); err != nil {
		return nil, err
	}
	cfg := newConfigFrom(opts)
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("cronjob: build jetstream context: %w", err)
	}
	stream, err := ensureStream(ctx, js)
	if err != nil {
		return nil, fmt.Errorf("cronjob: ensure stream %q: %w", streamName, err)
	}
	return &Runner{
		js:        js,
		stream:    stream,
		namespace: namespace,
		cfg:       cfg,
		jobs:      make(map[string]Job),
	}, nil
}

// Register adds jobs to the Runner, rejecting a nil job, a name or Schedule
// expression this package cannot honour, a name already registered on this
// Runner, and any call made after Run has started.
func (r *Runner) Register(jobs ...Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("cronjob: cannot Register a job after Run has started")
	}

	for _, j := range jobs {
		if j == nil {
			return errors.New("cronjob: cannot register a nil job")
		}
		name := j.Name()
		if err := validateName("job name", name); err != nil {
			return err
		}
		if err := validateSchedule(j.Schedule()); err != nil {
			return fmt.Errorf("cronjob: job %q: %w", name, err)
		}
		if _, exists := r.jobs[name]; exists {
			return fmt.Errorf("cronjob: job %q is already registered", name)
		}
		r.jobs[name] = j
	}
	return nil
}

// Run reconciles this namespace's broker state, provisions every registered
// job's Schedule and durable consumer, and dispatches Ticks until ctx is
// cancelled, returning ctx.Err() — never nil — on that path.
//
// It returns a different, non-nil error immediately if Run is called twice on
// the same Runner, called with no registered jobs, or if reconciliation or a
// job's Schedule or consumer cannot be provisioned.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("cronjob: Run has already been called on this Runner")
	}
	if len(r.jobs) == 0 {
		r.mu.Unlock()
		return errors.New("cronjob: Run called with no registered jobs; call Register before Run")
	}
	r.started = true
	jobs := make([]Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		jobs = append(jobs, j)
	}
	r.mu.Unlock()

	if err := r.reconcile(ctx, jobs); err != nil {
		return fmt.Errorf("cronjob: reconcile broker state: %w", err)
	}

	var consumeCtxs []jetstream.ConsumeContext
	defer func() {
		for _, cc := range consumeCtxs {
			cc.Stop()
		}
	}()

	for _, j := range jobs {
		if err := r.ensureSchedule(ctx, j); err != nil {
			return fmt.Errorf("cronjob: ensure schedule for job %q: %w", j.Name(), err)
		}
		cc, err := r.startConsumer(ctx, j)
		if err != nil {
			return fmt.Errorf("cronjob: start consumer for job %q: %w", j.Name(), err)
		}
		consumeCtxs = append(consumeCtxs, cc)
	}

	<-ctx.Done()
	return ctx.Err()
}

// scheduleSubject is where the Schedule definition of job is stored.
func (r *Runner) scheduleSubject(job string) string {
	return fmt.Sprintf("%s.%s.%s", schedulesSubjectPrefix, r.namespace, job)
}

// tickSubject is where the server generates job's Ticks.
func (r *Runner) tickSubject(job string) string {
	return fmt.Sprintf("%s.%s.%s", ticksSubjectPrefix, r.namespace, job)
}

// consumerName is the durable consumer every instance of job binds to.
func (r *Runner) consumerName(job string) string {
	return fmt.Sprintf("cron_%s_%s", r.namespace, job)
}

// ensureStream provisions the cronjobs stream, which is wholly owned by this
// package and useless without AllowMsgSchedules and AllowMsgTTL (ADR 0003).
func ensureStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{schedulesSubjectPrefix + ".>", ticksSubjectPrefix + ".>"},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		AllowMsgSchedules: true,
		AllowMsgTTL:       true,
	})
}

// reconcile converges the broker state of this Runner's namespace to what the
// code declares, so a removed or renamed job leaves no orphan generating Ticks
// forever (ADR 0003).
func (r *Runner) reconcile(ctx context.Context, jobs []Job) error {
	desiredSchedules := map[string]bool{}
	desiredTicks := map[string]bool{}
	for _, j := range jobs {
		desiredSchedules[r.scheduleSubject(j.Name())] = true
		desiredTicks[r.tickSubject(j.Name())] = true
	}

	if err := r.reconcileSchedules(ctx, desiredSchedules); err != nil {
		return err
	}
	return r.reconcileConsumers(ctx, desiredTicks)
}

// reconcileSchedules purges the stored definition of every orphan job, which
// is what stops the server generating further Ticks for its subject.
func (r *Runner) reconcileSchedules(ctx context.Context, desired map[string]bool) error {
	info, err := r.stream.Info(ctx, jetstream.WithSubjectFilter(schedulesSubjectPrefix+".>"))
	if err != nil {
		return fmt.Errorf("list schedule subjects: %w", err)
	}

	for subject := range info.State.Subjects {
		if !r.isOrphan(subject, schedulesSubjectPrefix, desired) {
			continue
		}
		if err := r.stream.Purge(ctx, jetstream.WithPurgeSubject(subject)); err != nil {
			return fmt.Errorf("purge orphan schedule %q: %w", subject, err)
		}
		r.cfg.logger.Info("cronjob: purged orphan cron schedule", "subject", subject)
	}
	return nil
}

// reconcileConsumers deletes the durable consumer of every orphan job.
func (r *Runner) reconcileConsumers(ctx context.Context, desired map[string]bool) error {
	var orphans []string
	// Consumers are matched by FilterSubject, which is unambiguously
	// dot-tokenised, and not by the underscore-joined consumer name.
	consumers := r.stream.ListConsumers(ctx)
	for info := range consumers.Info() {
		if !r.isOrphan(info.Config.FilterSubject, ticksSubjectPrefix, desired) {
			continue
		}
		orphans = append(orphans, info.Name)
	}
	if err := consumers.Err(); err != nil {
		return fmt.Errorf("list consumers: %w", err)
	}

	for _, name := range orphans {
		if err := r.stream.DeleteConsumer(ctx, name); err != nil {
			return fmt.Errorf("delete orphan consumer %q: %w", name, err)
		}
		r.cfg.logger.Info("cronjob: deleted orphan cron consumer", "consumer", name)
	}
	return nil
}

// isOrphan reports whether subject belongs to this Runner's namespace but no
// longer corresponds to a registered job.
func (r *Runner) isOrphan(subject, prefix string, desired map[string]bool) bool {
	ns, ok := namespaceFromSubject(subject, prefix)
	return ok && ns == r.namespace && !desired[subject]
}

// namespaceFromSubject extracts the namespace token of a subject shaped
// "<prefix>.<namespace>.<job>", reporting false for anything else.
func namespaceFromSubject(subject, prefix string) (string, bool) {
	rest, ok := strings.CutPrefix(subject, prefix+".")
	if !ok {
		return "", false
	}
	ns, _, found := strings.Cut(rest, ".")
	if !found || ns == "" {
		return "", false
	}
	return ns, true
}

// ensureSchedule converges the stored definition to the declared expression,
// republishing only when the expression changed, since a republish resets an
// `@every` phase (ADR 0003).
func (r *Runner) ensureSchedule(ctx context.Context, j Job) error {
	current, err := r.stream.GetLastMsgForSubject(ctx, r.scheduleSubject(j.Name()))
	switch {
	case err == nil:
		if current.Header.Get(jetstream.ScheduleHeader) == j.Schedule() {
			return nil
		}
	case errors.Is(err, jetstream.ErrMsgNotFound):
		// No definition stored yet: publish the first one.
	default:
		return err
	}

	return r.publishSchedule(ctx, j)
}

// publishSchedule writes the definition message whose Nats-Schedule header
// makes the server generate a Tick on the job's target subject at each firing.
func (r *Runner) publishSchedule(ctx context.Context, j Job) error {
	msg := &nats.Msg{
		Subject: r.scheduleSubject(j.Name()),
		Header:  nats.Header{},
	}
	msg.Header.Set(jetstream.ScheduleHeader, j.Schedule())
	msg.Header.Set(jetstream.ScheduleTargetHeader, r.tickSubject(j.Name()))
	// A cron expression has no single interval, so it keeps the server's
	// default Tick TTL (ADR 0003).
	if interval, ok := scheduleInterval(j.Schedule()); ok {
		msg.Header.Set(jetstream.ScheduleTTLHeader, (tickTTLMultiplier * interval).String())
	}

	_, err := r.js.PublishMsg(ctx, msg)
	return err
}

// startConsumer binds this instance to the job's durable consumer and
// dispatches every Tick it receives to the job's handler.
func (r *Runner) startConsumer(ctx context.Context, j Job) (jetstream.ConsumeContext, error) {
	cons, err := r.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       r.consumerName(j.Name()),
		FilterSubject: r.tickSubject(j.Name()),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		// One Tick in flight per job cluster-wide: a job never overlaps
		// itself, across replicas included (ADR 0003).
		MaxAckPending: 1,
	})
	if err != nil {
		return nil, err
	}

	name := j.Name()
	handler := j.Handler()
	return cons.Consume(func(msg jetstream.Msg) {
		handleTick(ctx, r.cfg.logger, r.namespace, name, handler, msg)
	})
}

// handleTick runs a job's handler for one Tick and always acks it, so a failed
// run is simply retried at the job's next Tick rather than redelivered
// (ADR 0003).
func handleTick(ctx context.Context, logger *slog.Logger, namespace, name string, handler Handler, msg jetstream.Msg) {
	if err := handler(ctx, buildTick(msg)); err != nil {
		logger.Error("cronjob: cron job handler failed",
			"namespace", namespace, "job", name, "error", err)
	}
	if err := msg.Ack(); err != nil {
		logger.Warn("cronjob: failed to ack tick",
			"namespace", namespace, "job", name, "error", err)
	}
}

// buildTick reads the Tick a handler receives out of the message the server
// generated for it.
func buildTick(msg jetstream.Msg) Tick {
	tick := Tick{}
	if md, err := msg.Metadata(); err == nil {
		tick.ScheduledAt = md.Timestamp
	}
	if next := msg.Headers().Get(jetstream.ScheduleNextHeader); next != "" && next != schedulePurgeSentinel {
		// RFC3339Nano parses timestamps with or without fractional seconds.
		if parsed, err := time.Parse(time.RFC3339Nano, next); err == nil {
			tick.NextAt = parsed
		}
	}
	return tick
}
