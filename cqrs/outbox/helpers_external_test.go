package outbox_test

import (
	"context"
	"sync"

	"github.com/ludusrusso/nats-cqrs/cqrs"
)

// testOrderCreated is a fixture Event type shared across the tests in this
// package: an embedded EventHeader with the required explicit
// json:"header" tag, exactly as the cqrs package's Marshal requires.
type testOrderCreated struct {
	cqrs.EventHeader `json:"header"`
	OrderID          string `json:"order_id"`
}

// recordingSink is a minimal, thread-safe cqrs.Sink stand-in for
// "NATS": it records every batch it is handed, and can be told to fail on
// demand, so tests can assert both on what the Forwarder delivered and on
// what happens when delivery fails.
type recordingSink struct {
	mu      sync.Mutex
	err     error
	batches [][]cqrs.Record
	fails   int
}

func newRecordingSink(err error) *recordingSink {
	return &recordingSink{err: err}
}

func (s *recordingSink) Publish(ctx context.Context, records ...cqrs.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		s.fails++
		return s.err
	}
	batch := make([]cqrs.Record, len(records))
	copy(batch, records)
	s.batches = append(s.batches, batch)
	return nil
}

// setErr changes the error future Publish calls return. Pass nil to make
// the sink healthy again.
func (s *recordingSink) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *recordingSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func (s *recordingSink) batchAt(i int) []cqrs.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches[i]
}

// recordCount returns the total number of Records across every batch
// delivered so far.
func (s *recordingSink) recordCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

// failCount returns how many Publish calls have returned an error so far.
func (s *recordingSink) failCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fails
}

// allIDs returns the ID of every Record delivered so far, across every
// batch, in delivery order.
func (s *recordingSink) allIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, b := range s.batches {
		for _, r := range b {
			ids = append(ids, r.ID)
		}
	}
	return ids
}
