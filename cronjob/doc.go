// Package cronjob runs Cron Jobs on NATS JetStream: a Cron Job is a named
// unit of recurring work whose Schedule is kept by the server, which
// generates a Tick at every firing and delivers it to exactly one of the
// instances bound to that job.
//
// The vocabulary follows the project glossary — see CONTEXT.md at the
// repository root — and the topology is recorded in
// docs/adr/0003-cron-jobs-on-jetstream-scheduled-messages.md. Read the ADR
// before changing anything here: the choices that look arbitrary in isolation
// (MaxAckPending: 1, the 5× Tick TTL, republishing a Schedule only when its
// expression changed, acking a Tick whose handler failed, and provisioning
// the cronjobs stream with CreateOrUpdateStream where natsjs.EnsureStreams
// refuses to reconfigure anything) are its direct consequences.
package cronjob
