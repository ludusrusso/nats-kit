package outbox

import (
	"context"
	"sync"

	"github.com/ludusrusso/nats-cqrs/cqrs"
)

// Memory implements both cqrs.Sink and Reader.
var (
	_ cqrs.Sink = (*Memory)(nil)
	_ Reader    = (*Memory)(nil)
)

// Memory is a reference, dependency-free implementation of both
// cqrs.Sink and Reader, backed by nothing but a slice guarded by a
// mutex — no database. It exists for two reasons: it makes this package,
// and application code built on it, testable with no database at all; and
// it is executable documentation of the read-delete-send-commit contract
// that Reader.ReadForSend otherwise only describes in prose. The comments
// inside ReadForSend below each name the step of that contract they stand
// in for.
//
// Memory is safe for concurrent use, including concurrent Publish and
// ReadForSend calls, and concurrent ReadForSend calls racing for the same
// pending Records: mirroring SELECT ... FOR UPDATE SKIP LOCKED, no two
// concurrent ReadForSend calls are ever handed the same Record, and a
// Record for which send fails is left exactly as it was, still pending,
// for a later pass to retry.
type Memory struct {
	mu      sync.Mutex
	entries []*memoryEntry
}

// memoryEntry pairs a pending Record with the one extra bit of state a
// real database gets for free from row-level locks: whether some in-flight
// ReadForSend call already holds it. held is Memory's entire stand-in for
// "SELECT ... FOR UPDATE SKIP LOCKED": a held entry is invisible to every
// other concurrent selection, exactly as a locked row would be.
type memoryEntry struct {
	record cqrs.Record
	held   bool
}

// NewMemory returns an empty Memory, ready to use.
func NewMemory() *Memory {
	return &Memory{}
}

// Publish implements cqrs.Sink: it appends records to the pending set.
//
// A real Outbox's Sink implementation runs this step inside the
// application's own database transaction, alongside the very state change
// that produced these Records — that is what makes the Outbox durable and
// transactional. Memory has no such transaction to join (it has no
// database at all), so here this is simply an in-memory append; binding an
// equivalent operation to a caller's own transaction is precisely the part
// this package leaves to the application. See the package doc.
func (m *Memory) Publish(ctx context.Context, records ...cqrs.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range records {
		m.entries = append(m.entries, &memoryEntry{record: r})
	}
	return nil
}

// Pending reports how many Records are currently waiting to be sent:
// published but not yet successfully drained by a committed ReadForSend
// call. It is provided for tests and diagnostics; nothing in this package
// requires a Reader implementation to expose an equivalent method.
func (m *Memory) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.entries {
		if !e.held {
			n++
		}
	}
	return n
}

// ReadForSend implements Reader. It follows the contract documented on
// Reader exactly; the comments below tie each block of code to the
// numbered step of that contract it plays the part of.
func (m *Memory) ReadForSend(ctx context.Context, batch int, send func(context.Context, []cqrs.Record) error) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if batch <= 0 {
		return 0, nil
	}

	// Steps 1-3: "open a transaction; SELECT ... FOR UPDATE SKIP LOCKED;
	// DELETE". Memory has no transaction to open and nothing to delete
	// from, so all three collapse into a single operation: claim up to
	// `batch` entries that are not already held by another in-flight call,
	// and mark them held. Marking an entry held both plays the role of the
	// row lock (other concurrent calls skip it below, exactly as
	// SKIP LOCKED would) and the role of the delete (Pending stops
	// counting it immediately, before send is even called — from every
	// other caller's point of view it is already gone).
	m.mu.Lock()
	picked := make([]*memoryEntry, 0, batch)
	for _, e := range m.entries {
		if len(picked) == batch {
			break
		}
		if e.held {
			// Another in-flight ReadForSend already holds this entry:
			// skip it and move on to the next one, rather than waiting.
			continue
		}
		e.held = true
		picked = append(picked, e)
	}
	m.mu.Unlock()

	if len(picked) == 0 {
		return 0, nil
	}

	records := make([]cqrs.Record, len(picked))
	for i, e := range picked {
		records[i] = e.record
	}

	// If send fails to return at all — including by panicking — picked
	// must still return to pending rather than stay held forever: a held
	// entry is invisible to Pending() and to every later pass, so a
	// panic that skipped an explicit unhold would strand it for good.
	// committed only becomes true once the commit branch below actually
	// removes these entries from m.entries; until then, this deferred
	// rollback runs on every exit path — normal error return, or a
	// panic unwinding out of send — so nothing can get stuck.
	committed := false
	defer func() {
		if !committed {
			m.mu.Lock()
			for _, e := range picked {
				e.held = false
			}
			m.mu.Unlock()
		}
	}()

	// Step 4: hand the batch over for sending. This is the one call in
	// this whole method that leaves the process — a real implementation
	// publishes to NATS here, across the network round trip the Reader
	// doc comment says is deliberately paid for. Everything else in this
	// method is bookkeeping around this single call.
	sendErr := send(ctx, records)
	if sendErr != nil {
		// Step 5, rollback branch: send failed, so nothing was actually
		// relinquished. The deferred rollback above undoes the hold and
		// leaves every picked entry exactly as it was — still pending,
		// still in its original position — so a later pass retries it.
		// This is the part that must be faithful above all else: on
		// failure, nothing is lost.
		return 0, sendErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Step 5, commit branch, and step 6: send succeeded, so the delete
	// from steps 1-3 becomes permanent. Drop every picked entry for good
	// and report how many Records were actually sent.
	remaining := make([]*memoryEntry, 0, len(m.entries)-len(picked))
	pickedSet := make(map[*memoryEntry]bool, len(picked))
	for _, e := range picked {
		pickedSet[e] = true
	}
	for _, e := range m.entries {
		if !pickedSet[e] {
			remaining = append(remaining, e)
		}
	}
	m.entries = remaining
	committed = true

	return len(picked), nil
}
