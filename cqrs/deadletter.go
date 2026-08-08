package cqrs

import (
	"encoding/json"
	"time"
)

// DeadLetter is the wire record a transport writes when a Handler has given
// up on a Message — either because it failed the same Message too many
// times in a row, or because it reported, via ErrUnprocessable, that no
// number of tries would ever help.
//
// Because Events fan out to every interested Handler independently, giving
// up belongs to a Handler, not to the Message: the same Event may be dead
// for one Handler while every other Handler handles it successfully. That
// is why Handler, not just Subject, is essential here.
type DeadLetter struct {
	// Subject is the original subject the Message was published on.
	Subject string `json:"subject"`
	// Handler is the name of the Handler that gave up.
	Handler string `json:"handler"`
	// Error is the last error the Handler returned.
	Error string `json:"error"`
	// Attempts is how many times the Handler was actually tried before
	// giving up — not a fixed ceiling. A Message that failed and was
	// redelivered until a transport's own retry limit was reached carries
	// that limit here, but a Message a Handler reported as permanently
	// unprocessable (see ErrUnprocessable) is dead-lettered on its very
	// first attempt, and Attempts is 1 for it: it must always say how many
	// tries actually happened, never imply a retry ladder that never ran.
	Attempts int `json:"attempts"`
	// FailedAt is when the Handler gave up.
	FailedAt time.Time `json:"failed_at"`
	// Envelope is the original envelope, verbatim.
	Envelope json.RawMessage `json:"envelope"`
}
