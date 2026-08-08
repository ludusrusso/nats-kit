package natsjs

import (
	"fmt"
	"unicode"
)

// durableName returns the durable consumer name for a Handler named
// handlerName running in serviceName: "serviceName_handlerName". See
// docs/adr/0001-commands-expire-and-new-consumers-start-from-now.md for why
// this identifies the Handler's position across restarts and replicas, and
// why renaming either half is not a cosmetic change.
//
// For an Event Handler, the ADR's own description of the consequence is
// complete: a rename is created fresh with DeliverNew and silently skips
// whatever was pending under the old name.
//
// For a Command Handler, renaming is worse than a silent skip: it is a
// startup failure. Commands run on DeliverAll consumers against a
// work-queue stream (see runHandler's DeliverPolicy comment), and a
// work-queue stream rejects two consumers whose filter subjects overlap.
// The old durable — still registered under its old name, since nothing
// deletes it — keeps exactly the same FilterSubject the newly named one
// would need, so CreateOrUpdateConsumer for the renamed Handler fails
// outright (JSConsumerWQConsumerNotUniqueErr, server error code 10100)
// until the old durable is deleted first. Runner.Run surfaces that as an
// ordinary error (see runHandler); it does not resolve itself.
func durableName(serviceName, handlerName string) string {
	return serviceName + "_" + handlerName
}

// validateDurableToken checks that name is safe to use as half of a
// JetStream durable consumer name: non-empty, and free of '.', '*', '>',
// whitespace, path separators and non-printable characters. label
// identifies name in the returned error (e.g. "service name", "handler
// name"), so a caller gets a clear, actionable message up front instead of
// a rejection from NATS itself once the invalid name reaches it.
func validateDurableToken(label, name string) error {
	if name == "" {
		return fmt.Errorf("natsjs: %s must not be empty", label)
	}
	for _, r := range name {
		if isInvalidDurableRune(r) {
			return fmt.Errorf(
				"natsjs: %s %q contains invalid character %q; "+
					"a durable consumer name cannot contain '.', '*', '>', "+
					"whitespace, path separators or non-printable characters",
				label, name, string(r),
			)
		}
	}
	return nil
}

func isInvalidDurableRune(r rune) bool {
	switch r {
	case '.', '*', '>', '/', '\\':
		return true
	}
	return unicode.IsSpace(r) || !unicode.IsPrint(r)
}
