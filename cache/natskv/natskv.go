// Package natskv is the cache.KV substrate every replica shares: a NATS
// JetStream KeyValue bucket whose MaxAge is the Cache's TTL.
//
// The vocabulary follows the project glossary — see CONTEXT.md at the
// repository root — and the choices here (one bucket per Cache, memory
// storage, no per-key TTL, the cache_ prefix) are recorded in
// docs/adr/0005-a-cache-entry-lives-as-long-as-its-substrate-says.md.
package natskv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-kit/cache"
)

// BucketPrefix namespaces every cache bucket in the broker: the physical
// bucket is cache_<name>, backed by the KV_cache_<name> stream.
const BucketPrefix = "cache_"

// Store implements cache.KV.
var _ cache.KV = (*Store)(nil)

// Config names and provisions the bucket a Store binds.
type Config struct {
	// Bucket is the logical name of the Cache; New prepends BucketPrefix, so
	// passing an already-prefixed name is an error rather than a doubled one.
	Bucket string
	// TTL is how long every entry of this bucket lives, as its MaxAge.
	TTL time.Duration
	// Replicas is the backing stream's replica count; zero means 1, the only
	// value a single-node broker accepts.
	Replicas int
}

// Store is a cache.KV backed by one JetStream KeyValue bucket.
type Store struct {
	kv jetstream.KeyValue
}

// New binds the bucket named by cfg on top of nc, creating or converging it,
// and rejects a Config without a TTL — a Cache that never evicts is a bug, not
// a configuration.
func New(ctx context.Context, nc *nats.Conn, cfg Config) (*Store, error) {
	if nc == nil {
		return nil, errors.New("natskv: nc must not be nil")
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, errors.New("natskv: bucket name is empty")
	}
	if strings.HasPrefix(cfg.Bucket, BucketPrefix) {
		return nil, fmt.Errorf("natskv: bucket %q already carries the %q prefix; pass the logical name", cfg.Bucket, BucketPrefix)
	}
	if cfg.TTL <= 0 {
		return nil, fmt.Errorf("natskv: bucket %q needs a TTL > 0", cfg.Bucket)
	}
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("natskv: build jetstream context: %w", err)
	}

	bucket := BucketPrefix + cfg.Bucket
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   bucket,
		TTL:      cfg.TTL,
		Replicas: replicas,
		// Cache entries are disposable, so a restart costs a cold cache and
		// nothing else (ADR 0005).
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("natskv: bind bucket %q: %w", bucket, err)
	}

	return &Store{kv: kv}, nil
}

// Get implements cache.KV, reporting a missing key as cache.ErrNotFound and
// leaving every other error to mean an unreachable broker.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	entry, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, cache.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

// Put implements cache.KV; the bucket's MaxAge is what evicts the entry later.
func (s *Store) Put(ctx context.Context, key string, value []byte) error {
	_, err := s.kv.Put(ctx, key, value)
	return err
}

// Delete implements cache.KV.
func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.kv.Delete(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}
