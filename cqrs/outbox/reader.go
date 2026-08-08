package outbox

import (
	"context"

	"github.com/ludusrusso/nats-kit/cqrs"
)

// Reader is the second of the two things an application must provide to
// have an Outbox. (The first is just cqrs.Publisher, used to accept
// Messages into the Outbox in the first place — see the package doc.)
// ReadForSend hands pending Records over for sending.
//
// # Why a callback, not a two-phase API
//
// ReadForSend takes a callback, send, instead of returning Records for the
// caller to act on and later confirm or abandon. That shape is deliberate:
// it is the direct expression of the Outbox glossary entry's central rule
// that handing over and sending are a single act — a Message is only
// relinquished once it has actually been sent. A two-phase API (return
// Records now, confirm or abandon them later, against a returned handle)
// would move the transaction's lifetime out of the implementation and into
// a contract the caller can get wrong: a forgotten handle is a leaked
// transaction. The callback form makes that failure structurally
// impossible, because the transaction's scope is exactly the callback's
// scope — there is no way to obtain a batch without also being on the hook
// to resolve it, synchronously, before ReadForSend returns.
//
// # The contract
//
// This is the contract every implementation MUST follow, in order. It is
// documented at length here, rather than left to be inferred, because every
// implementer depends on getting it exactly right:
//
//  1. Open its own database transaction. This library never sees a
//     transaction handle, a driver, or a dialect — only the application's
//     implementation of Reader ever opens or commits one.
//  2. Within that transaction, read up to batch pending Records, using
//     row-level locking that lets other, concurrent forwarders skip rows
//     already claimed by this call rather than blocking on them (SQL's
//     SELECT ... FOR UPDATE SKIP LOCKED is the canonical way to do this).
//     This is what lets several forwarder instances drain the same table
//     concurrently without contending with each other: each one simply
//     moves on to the rows nobody else is holding.
//  3. Delete the selected rows, inside the same transaction.
//  4. Call send with exactly the batch of Records just read.
//  5. Commit the transaction if, and only if, send returns nil. If send
//     returns an error, roll back instead — which puts the deleted rows
//     back exactly as they were — and propagate that error.
//  6. Return the number of Records actually sent (i.e. committed), and a
//     nil error; or, if send failed, 0 and the error that made the
//     transaction roll back.
//
// batch is a hint, not a promise: an implementation may return fewer
// Records than batch — including zero, when nothing is pending — but must
// never hand send more than batch.
//
// # A transaction held open across a network round trip, on purpose
//
// Step 4 runs while the transaction opened in step 1 is still open. That
// means a database transaction is held for as long as the call to send
// takes — typically a network round trip to NATS. This is a deliberate
// cost of the design, not an oversight (see ADR 0002 for the alternatives
// this was weighed against): it is what turns "hand the Records over" and
// "send the Records" into one indivisible act, instead of two acts that a
// crash between them could leave disagreeing about what actually happened.
//
// # Delivery is at-least-once
//
// One failure window survives this design regardless: send can succeed —
// the publish to NATS genuinely goes through — and then the commit in step
// 5 can itself fail (a dropped connection to the database, a crash between
// the two). When that happens the transaction rolls back, the Records
// return to pending, and the next pass sends them again, even though a
// copy already made it out. No design short of a distributed transaction
// spanning both the database and NATS can close this window, which is why
// every Message that leaves through the Outbox arrives at least once, not
// exactly once. The Forwarder mitigates the resulting duplicates by setting
// NATS' Nats-Msg-Id header from each Record's own ID, so JetStream's
// deduplication window discards the repeat — but only within that window,
// which the transport sets explicitly (2 minutes by default) and which
// this package plays no part in extending. A forwarder that is down, or an
// Outbox row stuck behind a database outage, for longer than the window
// republishes outside it, and that duplicate is genuinely delivered, not
// discarded. So Handlers must be idempotent unconditionally — not merely
// as a formality the dedup window happens to cover in the common case —
// and duplicates must be harmless.
//
// # Illustrative SQL
//
// The sketch below is documentation only — this package imports no SQL
// driver and contains no SQL of its own. An implementation over, say,
// PostgreSQL, might look like this:
//
//	tx, err := db.BeginTx(ctx, nil)
//	if err != nil {
//		return 0, err
//	}
//	defer tx.Rollback() // no-op once committed
//
//	rows, err := tx.QueryContext(ctx, `
//		SELECT row_id, subject, msg_id, data
//		FROM outbox
//		ORDER BY row_id
//		LIMIT $1
//		FOR UPDATE SKIP LOCKED
//	`, batch)
//	// ... scan rows into rowIDs []int64 and records []cqrs.Record ...
//
//	if len(records) == 0 {
//		return 0, tx.Commit() // nothing pending; commit the (empty) read
//	}
//
//	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE row_id = ANY($1)`, rowIDs); err != nil {
//		return 0, err // deferred Rollback undoes nothing yet; return early
//	}
//
//	if err := send(ctx, records); err != nil {
//		return 0, err // deferred Rollback fires: rows come back
//	}
//
//	if err := tx.Commit(); err != nil {
//		// send already succeeded: records were sent, but the delete did
//		// not stick. This is the at-least-once window described above.
//		return 0, err
//	}
//	return len(records), nil
type Reader interface {
	// ReadForSend opens a transaction, reads up to batch pending Records
	// using skip-locked-style row selection, deletes them, calls send with
	// that batch, and commits only if send returns nil — rolling back and
	// leaving the Records pending otherwise. See the Reader doc comment
	// for the full contract this method must satisfy. sent reports how
	// many Records were actually committed (i.e. sent); on failure sent is
	// 0, since nothing was actually relinquished.
	ReadForSend(ctx context.Context, batch int, send func(context.Context, []cqrs.Record) error) (sent int, err error)
}
