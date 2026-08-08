package cronjob

import (
	"context"
	"time"
)

// Tick is what a Cron Job's handler receives on each firing of its Schedule.
type Tick struct {
	// ScheduledAt is when the execution was due, i.e. when the server
	// generated this Tick.
	ScheduledAt time.Time
	// NextAt is when the next Tick of the same Schedule fires, zero when the
	// server advertised none.
	NextAt time.Time
}

// Handler runs a Cron Job when its Schedule fires.
type Handler = func(ctx context.Context, tick Tick) error

// Job is a Cron Job: a name, the Schedule expression it fires on, and the
// Handler to run on each Tick.
type Job interface {
	Name() string
	Schedule() string
	Handler() Handler
}

type job struct {
	name     string
	schedule string
	handler  Handler
}

// NewJob declares a Cron Job firing on the given Schedule expression — an
// `@every` duration, a 6-field cron, or a predefined alias — validated when
// the job is registered.
func NewJob(name, schedule string, handler Handler) Job {
	return &job{name: name, schedule: schedule, handler: handler}
}

func (j *job) Name() string     { return j.name }
func (j *job) Schedule() string { return j.schedule }
func (j *job) Handler() Handler { return j.handler }
