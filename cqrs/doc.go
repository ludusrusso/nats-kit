// Package cqrs defines the sealed Message contract, the wire format and
// the transport seam (Sink) that every other package in this library builds
// on — a NATS JetStream transport and a database-backed Outbox are both
// implemented on top of what is declared here. This package has zero
// external dependencies: standard library only, including in its tests.
//
// The vocabulary used throughout this package's names and doc comments —
// Message, Header, Command, Event, Message Name, Handler, Dead Letter,
// Outbox — follows the project glossary in CONTEXT.md at the repository
// root. Read that file first; it defines the concepts this package only
// implements.
package cqrs
