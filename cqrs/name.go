package cqrs

import (
	"fmt"
	"strings"
)

const (
	// commandSubjectPrefix and eventSubjectPrefix are prepended to a
	// Message Name to form the NATS subject it is published on.
	commandSubjectPrefix = "commands."
	eventSubjectPrefix   = "events."
)

// Namer lets a Message type override its Message Name, decoupling its wire
// identity from its Go type name — for example to rename a Go type without
// minting a new Message, or to give a Message a name independent of the Go
// package it happens to live in.
type Namer interface {
	Type() string
}

// MessageName returns the wire identity of m's Go type — the Message Name.
// Publisher and subscriber agree on a Message only through this name, so it
// is derived once, consistently, here.
//
// By default the Message Name is the Go type name, derived with
// fmt.Sprintf("%T", m) and trimmed of a leading "*" — this yields
// "packagename.TypeName", using the last path segment of the type's
// package. A type may override this default by implementing Namer
// (interface{ Type() string }); if it does, that name wins.
//
// The resulting name must be a safe, literal NATS subject token: it may
// contain only letters, digits, '_', '-' and '.', and must not be empty.
// This rejects NATS wildcards ('*', '>'), whitespace, and — importantly —
// unadorned generic type names such as "pkg.Foo[main.Bar]", which
// fmt.Sprintf would otherwise happily produce and which would silently
// mangle the resulting subject rather than fail loudly.
func MessageName(m Message) (string, error) {
	name := goTypeName(m)
	if n, ok := m.(Namer); ok {
		name = n.Type()
	}
	if err := validateSubjectToken(name); err != nil {
		return "", err
	}
	return name, nil
}

// goTypeName is the un-overridden, un-validated default Message Name.
func goTypeName(m Message) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", m), "*")
}

// Subject returns the NATS subject a Message is published on:
// "commands.<name>" for a Command, "events.<name>" for an Event, where
// <name> is m's Message Name (see MessageName).
func Subject(m Message) (string, error) {
	name, err := MessageName(m)
	if err != nil {
		return "", err
	}
	prefix, err := subjectPrefix(m)
	if err != nil {
		return "", err
	}
	return prefix + name, nil
}

// subjectPrefix reports whether m is a Command or an Event.
func subjectPrefix(m Message) (string, error) {
	switch m.(type) {
	case Command:
		return commandSubjectPrefix, nil
	case Event:
		return eventSubjectPrefix, nil
	default:
		// Unreachable in practice: Message is sealed (msg is unexported)
		// and its only implementors, CommandHeader and EventHeader, embed
		// cmd() and evt() respectively. Any concrete Message is therefore
		// always also a Command or an Event, never neither.
		return "", fmt.Errorf("cqrs: message of type %s is neither a Command nor an Event", goTypeName(m))
	}
}

// validateSubjectToken rejects anything that is not a safe, literal NATS
// subject token: empty strings, whitespace, the NATS wildcards '*' and '>',
// and any other character outside [A-Za-z0-9_-.].
func validateSubjectToken(name string) error {
	if name == "" {
		return fmt.Errorf("cqrs: message name must not be empty")
	}
	for _, r := range name {
		if !isSubjectTokenRune(r) {
			return fmt.Errorf(
				"cqrs: message name %q contains invalid character %q; "+
					"only letters, digits, '_', '-' and '.' are allowed "+
					"(this also rejects the NATS wildcards '*' and '>', whitespace, "+
					"and unadorned generic type names like \"pkg.Foo[main.Bar]\")",
				name, string(r),
			)
		}
	}
	return nil
}

func isSubjectTokenRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r == '_' || r == '-' || r == '.':
		return true
	default:
		return false
	}
}
