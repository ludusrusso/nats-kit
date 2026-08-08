package cache

import (
	"context"
	"sync"
	"time"
)

// Memory implements KV.
var _ KV = (*Memory)(nil)

// Memory is a reference, dependency-free KV backed by nothing but a map
// guarded by a mutex: it makes application code built on a Cache testable
// without a broker, and it is executable documentation of the KV contract.
// Being per-process, it gives no coordination between replicas — that is what
// cache/natskv is for.
type Memory struct {
	ttl time.Duration

	mu      sync.Mutex
	entries map[string]memoryEntry

	failure error
}

// memoryEntry is one stored value and the moment it stops being visible.
type memoryEntry struct {
	value     []byte
	expiresAt time.Time
}

// NewMemory returns an empty Memory whose entries live for ttl after each
// write; a non-positive ttl is a wiring mistake, not a runtime condition, so
// it panics.
func NewMemory(ttl time.Duration) *Memory {
	if ttl <= 0 {
		panic("cache: NewMemory needs a ttl > 0")
	}
	return &Memory{ttl: ttl, entries: make(map[string]memoryEntry)}
}

// SetFailure makes every subsequent operation fail with err until it is
// cleared with nil, standing in for an unreachable substrate.
func (m *Memory) SetFailure(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failure = err
}

// Get implements KV; expiry is enforced here, on read.
func (m *Memory) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return nil, m.failure
	}
	e, ok := m.entries[key]
	if !ok {
		return nil, ErrNotFound
	}
	if time.Now().After(e.expiresAt) {
		delete(m.entries, key)
		return nil, ErrNotFound
	}
	return append([]byte(nil), e.value...), nil
}

// Put implements KV.
func (m *Memory) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return m.failure
	}
	m.entries[key] = memoryEntry{
		value:     append([]byte(nil), value...),
		expiresAt: time.Now().Add(m.ttl),
	}
	return nil
}

// Delete implements KV.
func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return m.failure
	}
	delete(m.entries, key)
	return nil
}
