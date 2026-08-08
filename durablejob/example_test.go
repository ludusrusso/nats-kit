package durablejob_test

import (
	"context"
	"fmt"

	"github.com/ludusrusso/nats-kit/durablejob"
	"github.com/ludusrusso/nats-kit/natstest"
)

// Example dispatches one Durable Job and waits for it to report itself fully
// done.
//
// The dispatched message IS the job: the broker holds it, unacked, for as
// long as Execute runs, and hands it to another instance from the beginning
// if this one dies. Dispatch and execution are separate concerns here only
// for brevity — a Dispatcher holds no namespace of its own, so the service
// starting a job need not be the one that owns it.
func Example() {
	// An Example has no *testing.T, so it uses StartServer rather than
	// Start. In a real test, natstest.Start(t) is the one you want.
	nc, cleanup, err := natstest.StartServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	// quiet() discards the Runner's own logs so they stay out of this
	// example's output; a real service passes its own slog.Logger, or
	// nothing at all and gets slog.Default().
	runner, err := durablejob.New(ctx, nc, "billing", quiet())
	if err != nil {
		panic(err)
	}

	// Built before Run starts: both constructors converge the durablejobs
	// stream, and nats-server races a stream update against a concurrent
	// consumer create.
	dispatcher, err := durablejob.NewDispatcher(ctx, nc, quiet())
	if err != nil {
		panic(err)
	}

	done := make(chan string, 1)
	reindex := durablejob.NewJob("reindex_tenant", func(_ context.Context, payload []byte) error {
		// Execute must be re-runnable: a redelivery re-runs it from the
		// beginning, so a real job re-derives what is left from its own
		// durable state — rows still matching a predicate, objects still
		// missing — and never from the payload, which identifies the work
		// but never says how far it got.
		done <- string(payload)
		// Returning nil means FULLY done, and is what finally releases the
		// message. An error would be a genuine failure and redeliver.
		return nil
	})

	if err := runner.Register(reindex); err != nil {
		panic(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(ctx) }()

	// The job is durable from the moment it is dispatched: had no Runner
	// been up yet, the message would simply wait on the stream.
	if err := dispatcher.Dispatch(ctx, "billing", "reindex_tenant", []byte("tenant-1")); err != nil {
		panic(err)
	}

	tenant := <-done

	cancel()
	<-runErr

	fmt.Println("reindexed:", tenant)

	// Output:
	// reindexed: tenant-1
}
