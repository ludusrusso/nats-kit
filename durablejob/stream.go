package durablejob

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// streamName is the dedicated stream that isolates Durable Job traffic
	// from the Command and Event streams (ADR 0004).
	streamName = "durablejobs"

	jobsSubjectPrefix = streamName + ".jobs"
	dlqSubjectPrefix  = streamName + ".dlq"
)

// Headers a dead-lettered job carries, tracing it back to the original job
// message and the failure that parked it.
const (
	HeaderOriginSubject  = "Job-Origin-Subject"
	HeaderOriginSequence = "Job-Origin-Sequence"
	HeaderDeliveries     = "Job-Deliveries"
	HeaderError          = "Job-Error"
)

// JobSubject is the subject on which one message is one Durable Job of the
// given kind.
func JobSubject(namespace, job string) string {
	return fmt.Sprintf("%s.%s.%s", jobsSubjectPrefix, namespace, job)
}

// DLQSubject is the subject a job is parked on once it exhausts MaxDeliver
// genuine failures.
func DLQSubject(namespace, job string) string {
	return fmt.Sprintf("%s.%s.%s", dlqSubjectPrefix, namespace, job)
}

// consumerName is the durable consumer every replica of a job binds.
func consumerName(namespace, job string) string {
	return fmt.Sprintf("job_%s_%s", namespace, job)
}

// ensureStream converges the durablejobs stream, which this package wholly
// owns and therefore reconfigures rather than leaves alone (ADR 0004).
func ensureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{jobsSubjectPrefix + ".>", dlqSubjectPrefix + ".>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		return fmt.Errorf("durablejob: ensure stream %q: %w", streamName, err)
	}
	return nil
}

// validateName rejects tokens that would corrupt the dot-separated subject
// space or the underscore-joined consumer name.
func validateName(kind, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("durablejob: %s is empty", kind)
	}
	if strings.ContainsAny(name, ".*> \t") {
		return fmt.Errorf("durablejob: %s %q must not contain subject wildcards, dots or whitespace", kind, name)
	}
	return nil
}
