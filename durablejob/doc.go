// Package durablejob implements the Durable Job: long-running work that the
// broker holds open as one JetStream message until the job reports it is
// fully done.
//
// One message on the job subject is one job. It is acked only on completion,
// so a crash, a restart or a rolling deploy simply redelivers it. Execute is
// therefore re-run from the beginning on every delivery: it must re-derive
// its remaining work from durable state (rows still matching a predicate,
// objects still missing) and must never store a cursor in the message, whose
// payload only ever identifies the work.
//
// See CONTEXT.md at the repository root for the vocabulary, and
// docs/adr/0004-durable-jobs-redeliver-until-done.md for why the primitive is
// shaped this way.
package durablejob
