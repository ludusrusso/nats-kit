package cache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// ErrNotFound is what a KV reports for a key it does not hold: a plain miss,
// as opposed to any other error, which is the substrate being unreachable.
var ErrNotFound = errors.New("cache: key not found")

// KV is the substrate a Cache keeps its entries in, and the owner of how long
// they live: one static TTL shared by every entry it holds (ADR 0005).
type KV interface {
	// Get returns the value stored under key, or ErrNotFound when there is
	// none — including when it expired.
	Get(ctx context.Context, key string) ([]byte, error)
	// Put stores value under key, overwriting unconditionally.
	Put(ctx context.Context, key string, value []byte) error
	// Delete removes key, reporting success when the key was already absent.
	Delete(ctx context.Context, key string) error
}

// Loader produces a fresh value for a key the Cache does not hold.
type Loader[V any] func(ctx context.Context) (V, error)

// Cache is a single-flight, JSON-encoded cache of V over a KV.
type Cache[V any] struct {
	kv    KV
	valid func(V) bool

	mu       sync.Mutex
	inFlight map[string]*flight[V]
}

// flight is the one Loader call concurrent misses on the same key share.
type flight[V any] struct {
	done  chan struct{}
	value V
	err   error
}

// Option configures a Cache.
type Option[V any] func(*Cache[V])

// WithHitValidator installs a predicate every hit must pass; one that fails it
// is treated as a miss, which is how a value that can die before the
// substrate's TTL — an embedded token reaching its own expiry — heals itself.
func WithHitValidator[V any](valid func(V) bool) Option[V] {
	return func(c *Cache[V]) { c.valid = valid }
}

// New builds a Cache over kv, which is what decides how long its entries live.
func New[V any](kv KV, opts ...Option[V]) *Cache[V] {
	c := &Cache[V]{
		kv:       kv,
		inFlight: make(map[string]*flight[V]),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get returns the value held under key, calling loader when the Cache does not
// hold one and storing what it returns; concurrent misses on the same key
// share a single loader call, which runs under the context of whichever caller
// started it.
func (c *Cache[V]) Get(ctx context.Context, key string, loader Loader[V]) (V, error) {
	var zero V

	raw, err := c.kv.Get(ctx, key)
	switch {
	case err == nil:
		var v V
		if jsonErr := json.Unmarshal(raw, &v); jsonErr == nil {
			if c.valid == nil || c.valid(v) {
				return v, nil
			}
		}
		// A refused or undecodable entry is a miss: reload and overwrite it.
	case errors.Is(err, ErrNotFound):
		// An ordinary miss.
	default:
		// The substrate is unreachable, so serve from the loader uncached
		// rather than fail (ADR 0005).
		v, loadErr := loader(ctx)
		if loadErr != nil {
			return zero, loadErr
		}
		return v, nil
	}

	return c.singleflight(ctx, key, loader)
}

// singleflight calls loader once for key, handing its outcome to every caller
// that arrived while it was running.
func (c *Cache[V]) singleflight(ctx context.Context, key string, loader Loader[V]) (V, error) {
	var zero V

	c.mu.Lock()
	if f, ok := c.inFlight[key]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			return f.value, f.err
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	f := &flight[V]{done: make(chan struct{})}
	c.inFlight[key] = f
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.inFlight, key)
		c.mu.Unlock()
		close(f.done)
	}()

	value, err := loader(ctx)
	if err != nil {
		f.err = err
		return zero, err
	}
	f.value = value

	if encoded, encErr := json.Marshal(value); encErr == nil {
		// A failed write must not fail the call it rode in on: the next Get
		// simply misses again.
		_ = c.kv.Put(ctx, key, encoded)
	}
	return value, nil
}
