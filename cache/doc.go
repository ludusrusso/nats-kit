// Package cache is a Cache: a generic, single-flight, JSON-encoded cache of
// one value type over a pluggable KV substrate — a NATS JetStream KV bucket
// in cache/natskv, an in-process map in Memory.
//
// The vocabulary follows the project glossary — see CONTEXT.md at the
// repository root — and the shape is recorded in
// docs/adr/0005-a-cache-entry-lives-as-long-as-its-substrate-says.md. Read
// the ADR before changing anything here: the choices that look arbitrary in
// isolation (a KV contract with no per-entry TTL, one bucket per Cache,
// never caching a Loader's error, treating an unreachable KV as a reason to
// skip the cache rather than to fail) are its direct consequences.
package cache
