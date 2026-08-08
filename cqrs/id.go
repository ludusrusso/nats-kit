package cqrs

import (
	"crypto/rand"
	"encoding/hex"
)

const (
	// commandIDPrefix and eventIDPrefix let an ID's prefix alone reveal
	// whether it belongs to a Command or an Event, which is convenient in
	// logs and dashboards.
	commandIDPrefix = "cmd_"
	eventIDPrefix   = "evt_"

	// idRandomBytes is the amount of crypto/rand entropy hex-encoded into
	// each generated ID, after its prefix.
	idRandomBytes = 16
)

// newID generates an identifier: prefix followed by crypto/rand bytes,
// hex-encoded. rand.Read never returns an error and always fills its buffer
// entirely — per its doc comment, it crashes the program irrecoverably
// (rather than returning an error) if the OS's random source is ever
// unavailable — so there is no failure mode here for newID to degrade into
// instead of panicking, and no fallback path to write.
func newID(prefix string) string {
	var buf [idRandomBytes]byte
	rand.Read(buf[:])
	return prefix + hex.EncodeToString(buf[:])
}
