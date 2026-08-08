package durablejob

import "context"

// Job is a unit of long-running durable work, identified by its name and run
// by a Runner.
type Job interface {
	// Name identifies the job kind: with the Runner's namespace it
	// determines the job subject, the DLQ subject and the durable consumer.
	Name() string
	// Execute runs the job to completion for the dispatched payload; a nil
	// return means fully done, any error is a genuine failure (ADR 0004).
	Execute(ctx context.Context, payload []byte) error
}

type job struct {
	name    string
	execute func(ctx context.Context, payload []byte) error
}

// NewJob declares a Durable Job kind from a name and an Execute function.
func NewJob(name string, execute func(ctx context.Context, payload []byte) error) Job {
	return &job{name: name, execute: execute}
}

func (j *job) Name() string { return j.name }

func (j *job) Execute(ctx context.Context, payload []byte) error { return j.execute(ctx, payload) }
