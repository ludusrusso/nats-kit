package natscqrs_test

import (
	"context"
	"sync"

	"github.com/ludusrusso/nats-cqrs"
)

// testCreateOrder and testOrderCreated are fixture Message types shared
// across the external (black-box) tests in this package: they exercise the
// only supported declaration form — an embedded header with an explicit
// json:"header" tag.

type testCreateOrder struct {
	natscqrs.CommandHeader `json:"header"`
	CustomerID             string `json:"customer_id"`
}

type testOrderCreated struct {
	natscqrs.EventHeader `json:"header"`
	OrderID              string `json:"order_id"`
}

// fakeSink is a minimal, in-memory Sink: it just records every batch it is
// given, so tests can assert on batching behavior (one Publish call per
// bus call) as well as on the Records' contents.
type fakeSink struct {
	mu      sync.Mutex
	batches [][]natscqrs.Record
	err     error
}

func (s *fakeSink) Publish(ctx context.Context, records ...natscqrs.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	// Copy the batch: records is backed by the caller's slice.
	batch := make([]natscqrs.Record, len(records))
	copy(batch, records)
	s.batches = append(s.batches, batch)
	return nil
}

func (s *fakeSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}
