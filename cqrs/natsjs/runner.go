package natsjs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-cqrs/cqrs"
)

// Runner registers Handlers and runs them against NATS JetStream. For every
// registered Handler it creates (or resumes) a durable pull consumer named
// "serviceName_handlerName", pulls messages for it with bounded
// concurrency, and, depending on how the Handler's function returns, acks,
// redelivers (via a delayed Nak) or dead-letters each message.
//
// There is no application-level retry loop anywhere in Runner: once a
// Handler returns an error, the only thing Runner does is tell JetStream to
// redeliver later, or give up. Redelivery timing is entirely JetStream's
// job (see the AckWait/MaxDeliver consumer settings in Option).
//
// The one exception to "redeliver later, or give up after MaxDeliver" is a
// Handler error that wraps cqrs.ErrUnprocessable: Runner recognizes
// that regardless of how many delivery attempts remain and dead-letters the
// message immediately, on this very first attempt, instead of naking it —
// see handleMessage and cqrs.ErrUnprocessable's own doc comment for
// why. This package's own typedHandler.Handle already reports every
// decode failure it can detect on its own — before a Handler's registered
// function is ever invoked — this way.
//
// A Handler function that panics is treated exactly like one that returns
// an error: handleMessage recovers the panic, turns it into an error that
// carries the panic value and a stack trace, and runs it through the same
// nak/dead-letter path. A single Handler invocation going wrong — by
// returning an error or by panicking — never brings down the process; see
// handleMessage.
type Runner struct {
	js          jetstream.JetStream
	serviceName string
	cfg         *config

	mu       sync.Mutex
	started  bool
	handlers map[string]handlerEntry
}

// handlerEntry pairs a registered Handler with its resolved worker count
// (DefaultWorkers unless WithHandlerWorkers overrode it for this name).
type handlerEntry struct {
	handler cqrs.Handler
	workers int
}

// New builds a Runner for serviceName on top of nc, and ensures the streams
// it depends on exist (see EnsureStreams; an existing stream is never
// reconfigured) — exactly like NewPublisher, and for the same reason: ctx
// is here so that provisioning failure is reported by New itself, up
// front, rather than deferred to Run.
//
// serviceName becomes half of every durable consumer name this Runner
// creates (see durableName): it must be non-empty and free of '.', '*',
// '>', whitespace, path separators and non-printable characters, exactly
// like a Handler's name (see Register). New rejects a malformed
// serviceName immediately, rather than letting NATS reject the resulting
// durable name later with a much less specific error.
//
// New also rejects an Option whose value is nonsensical rather than merely
// unusual — e.g. WithMaxDeliver(0) — for the same reason: NATS would
// otherwise reject the resulting consumer config later, at Run, with a
// much less specific error (or, worse, silently accept a value that means
// something other than what it looks like it means; see
// WithMaxDeliver/config.validate).
func New(ctx context.Context, nc *nats.Conn, serviceName string, opts ...Option) (*Runner, error) {
	if nc == nil {
		return nil, errors.New("natsjs: nc must not be nil")
	}
	if err := validateDurableToken("service name", serviceName); err != nil {
		return nil, err
	}
	cfg := newConfigFrom(opts)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("natsjs: build jetstream context: %w", err)
	}
	if err := ensureStreams(ctx, js, cfg); err != nil {
		return nil, err
	}
	return &Runner{
		js:          js,
		serviceName: serviceName,
		cfg:         cfg,
		handlers:    make(map[string]handlerEntry),
	}, nil
}

// Register adds handlers to the Runner: each one is validated
// (Handler.Validate) and its name checked for durable-name safety, exactly
// like New checks serviceName. A Handler whose name collides with one
// already registered on this Runner is rejected — two Handlers cannot
// share the one durable consumer their shared name would produce.
//
// Register must be called before Run; calling it after Run has started
// returns an error instead of silently being ignored.
func (r *Runner) Register(handlers ...cqrs.Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("natsjs: cannot Register a handler after Run has started")
	}

	for _, h := range handlers {
		if h == nil {
			return errors.New("natsjs: cannot register a nil handler")
		}
		if err := h.Validate(); err != nil {
			return fmt.Errorf("natsjs: %w", err)
		}
		name := h.Name()
		if err := validateDurableToken("handler name", name); err != nil {
			return err
		}
		if _, exists := r.handlers[name]; exists {
			return fmt.Errorf("natsjs: handler %q is already registered", name)
		}
		r.handlers[name] = handlerEntry{handler: h, workers: r.cfg.workersFor(name)}
	}
	return nil
}

// Run creates a durable consumer for every registered Handler and pulls
// messages for each of them until ctx is cancelled. It does not itself
// provision the streams those consumers live on — New already did, before
// this Runner even existed (see EnsureStreams) — so if a stream this
// Runner depends on is missing or deleted by the time Run's consumers try
// to use it, that surfaces as an ordinary consumer-creation or publish
// error rather than Run silently re-creating it out from under whatever
// else might now be relying on its configuration.
//
// On cancellation, Run stops issuing new pull requests for every Handler.
// It does not merely let whatever single message is already being handled
// finish: each Handler drains its whole local prefetch buffer — up to that
// Handler's worker count worth of already-fetched messages, not just
// one — through the ordinary handling path before Run returns. With many
// workers (WithHandlerWorkers) that is materially more than "the" message.
// There is also no shutdown timeout of any kind: if a Handler hangs —
// never returning from Handle and never panicking, so the recover in
// handleMessage never triggers — Run blocks forever waiting for it,
// cancellation or not.
//
// Run returns ctx.Err() once ctx is cancelled and every Handler has
// finished draining — never nil on that path, matching
// outbox.Forwarder.Run's own documented convention, so a caller running
// both under one errgroup gets one shutdown convention to check, not two.
// Before that, Run returns a different, non-nil error immediately if: Run
// is called with no registered Handlers; a consumer could not be created
// for some Handler; a Handler's message iterator fails for a reason other
// than ordinary shutdown (see runHandler); or Run is called more than once
// on the same Runner. The first such Handler failure also cancels every
// other still-running Handler, so Run does not wait out ctx's full
// lifetime to report it.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return errors.New("natsjs: Run has already been called on this Runner")
	}
	if len(r.handlers) == 0 {
		r.mu.Unlock()
		return errors.New("natsjs: Run called with no registered handlers; call Register before Run")
	}
	r.started = true
	entries := make([]handlerEntry, 0, len(r.handlers))
	for _, he := range r.handlers {
		entries = append(entries, he)
	}
	r.mu.Unlock()

	// runCtx is cancelled either when ctx is (ordinary shutdown) or when
	// any single Handler's runHandler returns a genuine error (see below):
	// the latter is what lets Run report that failure promptly instead of
	// leaving every other Handler blocked on ctx.Done() until the caller's
	// own ctx eventually gets cancelled too.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errs := make([]error, len(entries))
	for i, he := range entries {
		wg.Add(1)
		go func(i int, he handlerEntry) {
			defer wg.Done()
			err := r.runHandler(runCtx, he)
			errs[i] = err
			if err != nil {
				cancel()
			}
		}(i, he)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return err
	}
	return ctx.Err()
}

// streamFor reports which of the three streams a Handler of kind consumes
// from.
func streamFor(kind cqrs.Kind) (string, error) {
	switch kind {
	case cqrs.KindCommand:
		return CommandsStreamName, nil
	case cqrs.KindEvent:
		return EventsStreamName, nil
	default:
		return "", fmt.Errorf("natsjs: handler has unknown kind %q", kind)
	}
}

// runHandler creates (or resumes) the durable consumer for he, then pulls
// and dispatches messages for it until ctx is cancelled and everything
// already dispatched has finished.
func (r *Runner) runHandler(ctx context.Context, he handlerEntry) error {
	streamName, err := streamFor(he.handler.Kind())
	if err != nil {
		return err
	}
	durable := durableName(r.serviceName, he.handler.Name())

	// DeliverPolicy: ADR 0001 calls for DeliverNew on every consumer this
	// library creates, Commands included — a Handler that comes into
	// existence later is not owed the past. That is exactly what happens
	// here for Events.
	//
	// For Commands it cannot be, and this is a hard JetStream server
	// constraint rather than a design choice this package makes: the
	// server unconditionally rejects any non-Direct, non-Sourcing consumer
	// on a WorkQueuePolicy stream whose DeliverPolicy isn't DeliverAll
	// (JSConsumerWQConsumerNotDeliverAllErr, error code 10101 — see
	// nats-server's server/consumer.go, the "Check on stream type
	// conflicts with WorkQueues" block). CreateOrUpdateConsumer fails
	// outright for every single command Handler if DeliverNew is used
	// here, which is not a viable transport.
	//
	// DeliverAll for Commands still honors the intent behind ADR 0001 and
	// the Command glossary entry in CONTEXT.md ("kept until it is handled
	// — or until it is old enough that doing it would no longer be
	// right"): a Command already queued when a new consumer is created is
	// work not yet done, so it is now correctly delivered instead of
	// silently orphaned — and the commands stream's MaxAge (see
	// EnsureStreams) still prunes anything old enough that acting on it
	// would no longer be right, before it ever reaches a consumer. What
	// changes from the ADR's literal text is only the mechanism: expiry
	// now also substitutes for skipping backlog, rather than merely
	// cleaning up backlog DeliverNew would otherwise have orphaned.
	//
	// This DeliverPolicy choice is fixed at first consumer creation only;
	// CreateOrUpdateConsumer on a Runner restart resumes the existing
	// durable's stored position, as usual.
	deliverPolicy := jetstream.DeliverNewPolicy
	if he.handler.Kind() == cqrs.KindCommand {
		deliverPolicy = jetstream.DeliverAllPolicy
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: he.handler.Subject(),
		DeliverPolicy: deliverPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       r.cfg.ackWait,
		MaxDeliver:    r.cfg.maxDeliver,
		// Exactly he.workers messages in flight at once: this is the
		// consumer-side half of the "one worker by default" guarantee: the
		// client-side half is the semaphore below, and PullMaxMessages
		// matching he.workers so the client never even buffers more than
		// it could dispatch.
		MaxAckPending: he.workers,
	}

	cons, err := r.js.CreateOrUpdateConsumer(ctx, streamName, consumerCfg)
	if err != nil {
		return fmt.Errorf("natsjs: create consumer %q on stream %q: %w", durable, streamName, err)
	}

	r.cfg.logger.Info("natsjs: consumer ready",
		"handler", he.handler.Name(),
		"durable", durable,
		"stream", streamName,
		"subject", he.handler.Subject(),
		"workers", he.workers,
	)

	iter, err := cons.Messages(jetstream.PullMaxMessages(he.workers))
	if err != nil {
		return fmt.Errorf("natsjs: start pulling messages for consumer %q: %w", durable, err)
	}
	// Stop unconditionally on the way out, whichever path gets us there.
	// On the ordinary shutdown path below (ctx cancelled, Drain already
	// called, iterator already closed) this is a harmless no-op — Stop and
	// Drain both only ever take effect once. On every other path — an
	// error returned from Next below — nothing else in this function has
	// stopped the iterator yet, and skipping that would leak the pull
	// subscription for good: this is exactly what used to happen for
	// ErrNoHeartbeat, where the client would otherwise keep a dead
	// subscription (and its background goroutines) around forever.
	defer iter.Stop()

	// Drain (not Stop) on ctx cancellation: it stops issuing new pull
	// requests to the server immediately, but still hands over, via Next
	// below, whatever the client already fetched into its local buffer —
	// so a message that arrived just before shutdown is still processed
	// rather than discarded and left to redeliver.
	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			iter.Drain()
		case <-stopWatcher:
		}
	}()
	defer close(stopWatcher)

	sem := make(chan struct{}, he.workers)
	var wg sync.WaitGroup

	for {
		msg, err := iter.Next()
		if err != nil {
			// Let every message already dispatched finish before deciding
			// what to report, on this path exactly as on the loop's normal
			// exit: an iterator failure and a cancellation both mean "no
			// more new messages", never "abandon what is in flight".
			wg.Wait()

			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				// The only path that produces this: ctx was cancelled and
				// the watcher above called Drain. Ordinary shutdown, not a
				// failure — Run relies on this returning nil so it does not
				// mistake a clean stop for the kind of error that should
				// cancel every other Handler's runHandler too.
				return nil
			}

			// Anything else is a real transport failure this Handler
			// cannot recover from on its own: a deleted consumer
			// (ErrConsumerDeleted), a rejected request (ErrBadRequest), a
			// server that stopped sending heartbeats for ~30s
			// (ErrNoHeartbeat, since Messages defaults
			// ReportMissingHeartbeats to true), or the connection closing
			// outright. Silently breaking out of the loop and returning nil
			// here — this function's previous behavior — left the Handler
			// permanently, silently stopped: Run reported a clean
			// shutdown while this Handler had actually already given up.
			r.cfg.logger.Error("natsjs: message iterator for consumer stopped unexpectedly",
				"handler", he.handler.Name(), "durable", durable, "error", err)
			return fmt.Errorf("natsjs: consumer %q on stream %q: message iterator stopped: %w", durable, streamName, err)
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(msg jetstream.Msg) {
			defer wg.Done()
			defer func() { <-sem }()
			r.handleMessage(ctx, he, msg)
		}(msg)
	}
}

// handleMessage invokes he.handler against msg and settles msg according
// to the outcome: Ack on success, a delayed Nak on an ordinary failure that
// still has delivery attempts left, or a Dead Letter — settled with Term,
// not Ack; see deadLetter's doc comment for why — once either NumDelivered
// has reached MaxDeliver or he.handler has reported, via
// cqrs.ErrUnprocessable, that this Message will never succeed
// regardless of how many attempts remain.
func (r *Runner) handleMessage(ctx context.Context, he handlerEntry, msg jetstream.Msg) {
	// A Handler must be allowed to finish even after Runner.Run's ctx is
	// cancelled — graceful shutdown means letting in-flight work complete,
	// not aborting it — so the Handler runs against a context detached
	// from ctx's cancellation (but still carrying its values, if any).
	handleCtx := context.WithoutCancel(ctx)

	name := he.handler.Name()
	subject := msg.Subject()

	meta, err := msg.Metadata()
	if err != nil {
		r.cfg.logger.Error("natsjs: failed to read message metadata; leaving it for redelivery",
			"handler", name, "subject", subject, "error", err)
		return
	}
	// attempts counts every delivery JetStream has made for this message so
	// far (MsgMetadata.NumDelivered), which is not quite the same claim as
	// "how many times the Handler function ran": a delivery that timed out
	// via AckWait without he.handler.Handle ever being called also
	// increments it, same as one that ran and failed or panicked. This is
	// the same value cqrs.DeadLetter.Attempts is populated from below.
	attempts := int(meta.NumDelivered)

	handleErr := callHandle(handleCtx, he.handler, msg.Data())
	if handleErr == nil {
		if err := msg.Ack(); err != nil {
			r.cfg.logger.Error("natsjs: failed to ack a successfully handled message",
				"handler", name, "subject", subject, "error", err)
		}
		return
	}

	// Checked before the attempts-based branch below, and regardless of how
	// many attempts remain: a Handler that wraps cqrs.ErrUnprocessable
	// is declaring this exact Message unprocessable by construction — no
	// amount of waiting or redelivery would ever turn this particular
	// failure into a success (this package's own typedHandler.Handle wraps
	// it for exactly this reason, before he.handler's registered function
	// is even reached, for a corrupt envelope, a Message Name mismatch, or
	// a payload that will never unmarshal). Dead-lettering immediately, on
	// attempt 1, is therefore correct even though attempts < maxDeliver —
	// this is the one path where that inequality does not mean "retry
	// again": see deadLetter's own doc comment for what attempts means once
	// it reaches that function.
	if errors.Is(handleErr, cqrs.ErrUnprocessable) {
		r.cfg.logger.Warn("natsjs: handler reported a permanent failure; dead-lettering without retrying",
			"handler", name,
			"subject", subject,
			"attempt", attempts,
			"error", handleErr,
		)
		r.deadLetter(handleCtx, he, msg, handleErr, attempts)
		return
	}

	if attempts >= r.cfg.maxDeliver {
		r.deadLetter(handleCtx, he, msg, handleErr, attempts)
		return
	}

	r.cfg.logger.Warn("natsjs: handler failed, message will be redelivered",
		"handler", name,
		"subject", subject,
		"attempt", attempts,
		"max_deliver", r.cfg.maxDeliver,
		"error", handleErr,
	)

	if err := r.nak(msg); err != nil {
		r.cfg.logger.Error("natsjs: failed to nak message", "handler", name, "subject", subject, "error", err)
	}
}

// callHandle invokes handler.Handle against data and recovers a panic from
// it, turning it into an ordinary error instead of letting it propagate.
//
// A Handler is arbitrary user code running on a goroutine this package
// spawned (see runHandler); without this recover, a single Handler
// invocation panicking would unwind straight through handleMessage and
// runHandler, past this package's own logging and settlement logic
// entirely, and take down the whole process — every other Handler this
// Runner is running included, and every other Runner in the same process.
// The in-flight message would end up neither acked nor naked, left to
// redeliver only once AckWait eventually times it out. Converting the
// panic into an error instead routes it through exactly the same nak/dead-
// letter path as an ordinary returned error: a Handler that panics gets
// retried up to MaxDeliver times and then dead-lettered, same as one that
// returns an error every time, and every other Handler and Runner in the
// process is unaffected.
func callHandle(ctx context.Context, handler cqrs.Handler, data []byte) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("cqrs: handler %q panicked: %v\n%s", handler.Name(), rec, debug.Stack())
		}
	}()
	return handler.Handle(ctx, data)
}

// nak negatively acknowledges msg, delaying redelivery by the Runner's
// configured AckWait — the same delay a silent AckWait timeout would use —
// so a Handler's explicit failure redelivers on the same schedule a
// Handler that hangs or crashes without acking or naking at all would get.
//
// A bare Msg.Nak() deliberately is not used here: per its own
// documentation, Nak "does not adhere to AckWait ... and triggers instant
// redelivery", which would turn AckWait into dead configuration and
// produce a tight redelivery loop on every explicit Handler failure.
// NakWithDelay is what actually makes AckWait's spacing apply to the
// failure path too.
func (r *Runner) nak(msg jetstream.Msg) error {
	return msg.NakWithDelay(r.cfg.ackWait)
}

// dlqPublishRetries, dlqPublishRetryDelay and dlqPublishAttemptTimeout
// bound how hard deadLetter tries to publish a Dead Letter before giving
// up on this attempt and nak-ing the original message instead (see
// deadLetter). Together they ride out a brief blip — a leader election, a
// momentary network hiccup — without either blocking a Handler's worker
// goroutine indefinitely or declaring defeat on the first transient error.
//
// dlqPublishAttemptTimeout specifically bounds a single attempt: msg's
// handleCtx (see handleMessage) deliberately has no deadline of its own —
// a Handler must be allowed to finish even after Run's ctx is
// cancelled — so without a timeout applied here, one slow or wedged
// publish attempt could block this goroutine for as long as JetStream's
// own default request timeout allows (5s per attempt at the time of
// writing), times dlqPublishRetries. Capping each attempt keeps the whole
// retry sequence's worst case bounded and predictable instead of
// inheriting whatever a client library's default happens to be.
//
// dlqPublishRetryDelay is reused, deliberately, as the delay for the
// fallback Nak below when every retry still fails: by the time deadLetter
// runs, attempts has already reached MaxDeliver, so nats-server will not
// actually redeliver msg again regardless of what delay is requested (see
// deadLetter's doc comment) — the delay argument only controls how soon
// the server settles its own bookkeeping for msg (frees the MaxAckPending
// slot msg was occupying). A short, fixed delay here settles that promptly
// regardless of how large this Runner's AckWait happens to be configured,
// rather than tying an unrelated cleanup to a duration that exists for a
// completely different purpose.
const (
	dlqPublishRetries        = 3
	dlqPublishRetryDelay     = 200 * time.Millisecond
	dlqPublishAttemptTimeout = 2 * time.Second
)

// deadLetter builds a cqrs.DeadLetter for msg — he's Handler has given
// up on it, either because it exhausted MaxDeliver or because it wrapped
// cqrs.ErrUnprocessable and gave up on the very first attempt — and
// publishes it to "dlq.<original subject>".
//
// Because Events fan out to every interested Handler independently, giving
// up belongs to a Handler, not to the message: the DLQ is keyed on the
// original subject, and the Handler's name travels inside the DeadLetter,
// so the same Event can be dead for this Handler while every other Handler
// handles it successfully.
//
// attempts is handed in by the caller rather than recomputed here, and
// means exactly what cqrs.DeadLetter.Attempts documents: how many times
// the Handler was actually tried. On the exhausted-MaxDeliver path that is
// already equal to MaxDeliver by construction. On the ErrUnprocessable
// fast path it is not — it is however many deliveries (almost always
// exactly one) it took before the Handler reported the failure as
// permanent — and DeadLetter.Attempts must say so truthfully rather than
// implying a full retry ladder that never ran.
//
// That difference matters for what happens below if the Dead Letter itself
// cannot be published (see the two Nak fallbacks). On the
// exhausted-MaxDeliver path, nats-server enforces MaxDeliver again,
// independently, at the moment it is about to actually redeliver a message
// (see getNextMsg in nats-server's server/consumer.go checking delivery
// count against maxdc right before handing a message to a puller) — so
// neither an AckWait timeout nor an explicit Nak brings this exact message
// back to any consumer again, ever; only nats-server's own "max deliveries
// exceeded" advisory and internal bookkeeping cleanup follow. (Measured
// directly against this module's nats-server version — see the test for
// this fix.) On the ErrUnprocessable fast path, that guarantee does not
// hold: attempts can be well below MaxDeliver, so a Nak fallback here can
// genuinely schedule a real redelivery. That is an accepted, deliberate
// trade-off, not an oversight: it only happens once publishDeadLetter has
// already retried and failed, i.e. the DLQ itself is unavailable, and
// letting the message come back — to fail the same way and try
// dead-lettering again once the DLQ recovers — loses nothing and beats the
// alternative of a bare Term settling it with no durable record anywhere
// that it was ever unprocessable.
//
// So when the Dead Letter cannot be published, msg is explicitly Nak'd not
// to request a delivery that (on the exhausted-MaxDeliver path) will not
// happen, but to settle msg's bookkeeping deliberately and promptly instead
// of leaving it neither acked nor naked: the previous version of this code
// did the latter and then logged "leaving message pending for
// redelivery", which was false on that path — there is no redelivery
// coming — and left msg's AckPending slot occupied for however long it
// took nats-server's own AckWait-driven sweep to notice and clean it up on
// its own. msg itself still physically remains in the stream either
// way — that is unavoidable without settling it ourselves, which we
// deliberately do not do for a message we could not actually record as
// dead-lettered — until the stream's MaxAge eventually evicts it, exactly
// as ADR 0001 already intends for an undeliverable Command. What this fix
// actually changes is: a transient publish failure (the common case) now
// gets retried and usually still succeeds, instead of the very first blip
// condemning msg to that fate; and a persistent one is logged accurately,
// loudly, every time some message reaches this path while the underlying
// problem lasts — never silently, and never with a claim about what
// happens next that is not true.
//
// Once the Dead Letter is actually published, msg is settled with Term,
// not Ack. Both were measured directly against this module's embedded
// nats-server (see the natsjs package's tests): on the commands stream
// (WorkQueuePolicy), Term removes msg from the stream exactly like Ack
// does — nats-server's own consumer code treats a Term "like an ack to
// suppress redelivery" for the purpose of work-queue cleanup — and on a
// LimitsPolicy stream (events, and the DLQ stream itself), neither Term
// nor Ack removes msg; both merely stop redelivery, and msg stays until
// the stream's own MaxAge evicts it, exactly as intended. So the two are
// operationally equivalent here, but not semantically: Ack claims this
// Handler successfully processed msg, which is false — it gave up — while
// Term is JetStream's explicit "do not redeliver this, I am done with it,
// I am not claiming success," which is what actually happened, and which
// also emits a distinct delivery-terminated advisory Ack does not.
func (r *Runner) deadLetter(ctx context.Context, he handlerEntry, msg jetstream.Msg, handleErr error, attempts int) {
	name := he.handler.Name()
	subject := msg.Subject()

	envelope := make(json.RawMessage, len(msg.Data()))
	copy(envelope, msg.Data())

	dl := cqrs.DeadLetter{
		Subject:  subject,
		Handler:  name,
		Error:    handleErr.Error(),
		Attempts: attempts,
		FailedAt: time.Now().UTC(),
		Envelope: envelope,
	}

	data, err := json.Marshal(dl)
	if err != nil {
		// This is reachable, not merely theoretical: json.RawMessage's
		// contract requires its bytes to already be valid JSON, and
		// Envelope is msg.Data() verbatim — so a message whose body was
		// never valid JSON to begin with (external corruption, a
		// misbehaving publisher that is not this package) fails right
		// here, deterministically, every time. Retrying json.Marshal on
		// the same input would just fail identically again, so there is
		// no retry loop for this path unlike the publish below. Nak
		// anyway (see the doc comment above for what that does and does
		// not achieve at this point) so msg is settled deliberately, and
		// this failure is logged accurately, instead of leaving msg
		// neither acked nor naked while claiming otherwise. The
		// consequence worth naming plainly: a message whose body is not
		// valid JSON can never actually reach the Dead Letter stream —
		// cqrs.DeadLetter has no way to carry it — it only ever gets
		// this Nak-and-log fallback.
		r.cfg.logger.Error("natsjs: failed to marshal dead letter; nak-ing to settle it, since it cannot be dead-lettered and will not be redelivered either way",
			"handler", name, "subject", subject, "error", err)
		if nakErr := msg.NakWithDelay(dlqPublishRetryDelay); nakErr != nil {
			r.cfg.logger.Error("natsjs: failed to nak an un-dead-letterable message", "handler", name, "subject", subject, "error", nakErr)
		}
		return
	}

	dlqSubject := DeadLetterSubjectPrefix + subject
	// A retried Dead Letter publish — by the loop inside publishDeadLetter
	// just below — should deduplicate into the same DLQ record rather than
	// produce a second one for the same failure. The original message's
	// own ID plus this Handler's name is a natural key for that: it
	// identifies "this Handler gave up on this message", which is exactly
	// the fact one Dead Letter records. dlqMsgID returns "" if the
	// original ID cannot be recovered from the envelope, in which case the
	// publish still goes out, just without deduplication — same fallback
	// Publisher uses for a Record with no ID.
	msgID := dlqMsgID(msg.Data(), name)
	if msgID == "" {
		r.cfg.logger.Warn("natsjs: dead-lettering a message with no recoverable ID; a retried publish of it cannot be deduplicated",
			"handler", name, "subject", subject)
	}

	if err := r.publishDeadLetter(ctx, dlqSubject, data, msgID); err != nil {
		r.cfg.logger.Error("natsjs: failed to publish dead letter after retrying; nak-ing to settle it, since it cannot be dead-lettered and will not be redelivered either way",
			"handler", name, "subject", subject, "dlq_subject", dlqSubject, "error", err)
		if nakErr := msg.NakWithDelay(dlqPublishRetryDelay); nakErr != nil {
			r.cfg.logger.Error("natsjs: failed to nak an un-dead-letterable message", "handler", name, "subject", subject, "error", nakErr)
		}
		return
	}

	r.cfg.logger.Error("natsjs: handler gave up on this message; message dead-lettered",
		"handler", name,
		"subject", subject,
		"dlq_subject", dlqSubject,
		"attempts", attempts,
		"error", handleErr,
	)

	if err := msg.Term(); err != nil {
		r.cfg.logger.Error("natsjs: failed to term dead-lettered message",
			"handler", name, "subject", subject, "error", err)
	}
}

// publishDeadLetter publishes data to dlqSubject, retrying up to
// dlqPublishRetries times, each attempt bounded by dlqPublishAttemptTimeout
// and separated by dlqPublishRetryDelay, before reporting failure back to
// deadLetter. msgID, if non-empty, is sent as Nats-Msg-Id so a retried
// attempt deduplicates instead of writing a second Dead Letter record for
// the same failure.
func (r *Runner) publishDeadLetter(ctx context.Context, dlqSubject string, data []byte, msgID string) error {
	var opts []jetstream.PublishOpt
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}

	var lastErr error
	for attempt := 1; attempt <= dlqPublishRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, dlqPublishAttemptTimeout)
		_, err := r.js.Publish(attemptCtx, dlqSubject, data, opts...)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == dlqPublishRetries {
			break
		}
		select {
		case <-time.After(dlqPublishRetryDelay):
		case <-ctx.Done():
			return lastErr
		}
	}
	return lastErr
}

// dlqMsgID derives the Nats-Msg-Id for a Dead Letter record from the
// original message's own ID — read out of the envelope's embedded header,
// the same "header" object every Message's CommandHeader/EventHeader
// marshals into — and handlerName, so that a Dead Letter is deduplicated
// per (message, Handler) pair, matching the DeadLetter glossary entry:
// giving up belongs to a Handler, not to the message. It returns "" if the
// original ID cannot be recovered, e.g. because rawEnvelope is not the
// well-formed envelope this package itself always produces.
func dlqMsgID(rawEnvelope []byte, handlerName string) string {
	var env struct {
		Payload struct {
			Header struct {
				ID string `json:"id"`
			} `json:"header"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(rawEnvelope, &env); err != nil || env.Payload.Header.ID == "" {
		return ""
	}
	return env.Payload.Header.ID + "_" + handlerName
}
