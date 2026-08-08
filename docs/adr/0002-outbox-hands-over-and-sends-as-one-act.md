# Outbox hands over and sends as one act

The Outbox is defined by what the application must provide: a way to accept Messages, and a way to hand them over for sending. We expose the second as a single operation, `ReadForSend`, that takes a callback rather than returning messages for the caller to act on afterward. The application's implementation opens its own transaction, reads the pending Messages, deletes them, invokes the callback — which publishes to NATS — and commits only if the callback returned no error. The library never sees a transaction handle, a driver, or a dialect; only the application's implementation opens and commits transactions. Because the delete only commits after a successful publish, a Message is relinquished exactly when the Outbox glossary entry requires: once it has been sent. If NATS is unreachable, the rollback puts the rows back and no Message is lost. The one hole this leaves — publish succeeds, but the commit itself then fails — makes delivery at-least-once, which no non-distributed-transaction design avoids; the forwarder mitigates it by setting `Nats-Msg-Id` from the Message's identity, so JetStream's deduplication window discards the resulting repeat.

## Considered Options

- **Mark-as-sent** instead of read-and-delete: read the batch, publish, then update a "sent" flag in a separate transaction. Rejected: it has the exact same at-least-once failure mode as read-and-delete, while additionally requiring a growing table and an ongoing pruning process to remove sent rows.
- **Two-phase API** — `ReadForSend` returns messages plus a handle the caller later confirms or abandons. Rejected: it moves the transaction's lifetime out of the implementation and into a contract the caller can get wrong. A forgotten handle is a leaked transaction. The callback form makes that failure structurally impossible, since the transaction's scope is the callback's scope.

## Consequences

- A database transaction is held open across a network round trip to NATS. This is a deliberate cost, not an oversight. Implementations should select their batch with row-level locking that lets other forwarders skip locked rows, so concurrent forwarders do not contend with each other.
- Handlers must be idempotent. Duplicates are possible under this design and must be harmless.
- The Outbox provides no ordering guarantee: concurrent forwarders and retried batches can reorder Messages relative to the order in which they were written.
