package natskv_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ludusrusso/nats-kit/cache"
	"github.com/ludusrusso/nats-kit/cache/natskv"
	"github.com/ludusrusso/nats-kit/natstest"
)

// newStore starts a broker of its own and binds one bucket on it.
func newStore(t *testing.T, bucket string, ttl time.Duration) (*nats.Conn, *natskv.Store) {
	t.Helper()
	nc, cleanup := natstest.Start(t)
	t.Cleanup(cleanup)

	store, err := natskv.New(context.Background(), nc, natskv.Config{Bucket: bucket, TTL: ttl})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return nc, store
}

// waitUntil polls cond every 10ms until it reports true, or fails the test
// after timeout.
func waitUntil(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewRequiresATTL(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	if _, err := natskv.New(context.Background(), nc, natskv.Config{Bucket: "nottl"}); err == nil {
		t.Fatal("New accepted a Config without a TTL")
	}
}

func TestNewPrefixesTheBucketName(t *testing.T) {
	nc, _ := newStore(t, "prefixed", time.Minute)
	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.KeyValue(ctx, "cache_prefixed"); err != nil {
		t.Fatalf("bucket cache_prefixed: %v", err)
	}
	if _, err := js.Stream(ctx, "KV_cache_prefixed"); err != nil {
		t.Fatalf("stream KV_cache_prefixed: %v", err)
	}
}

func TestNewRejectsAnAlreadyPrefixedBucket(t *testing.T) {
	nc, cleanup := natstest.Start(t)
	defer cleanup()

	_, err := natskv.New(context.Background(), nc, natskv.Config{Bucket: "cache_twice", TTL: time.Minute})
	if err == nil {
		t.Fatal("New accepted an already-prefixed bucket name, which would bind cache_cache_twice")
	}
}

func TestGetReportsAMissingKeyAsErrNotFound(t *testing.T) {
	_, store := newStore(t, "miss", time.Minute)

	if _, err := store.Get(context.Background(), "absent"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("got %v, want cache.ErrNotFound", err)
	}
}

func TestPutThenGetReturnsTheValue(t *testing.T) {
	_, store := newStore(t, "roundtrip", time.Minute)
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestPutOverwrites(t *testing.T) {
	_, store := newStore(t, "overwrite", time.Minute)
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("first")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := store.Put(ctx, "k", []byte("second")); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	got, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want the last write", got)
	}
}

func TestDeleteRemovesTheKeyAndAcceptsAnAbsentOne(t *testing.T) {
	_, store := newStore(t, "delete", time.Minute)
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, "k"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("got %v after Delete, want cache.ErrNotFound", err)
	}

	if err := store.Delete(ctx, "never"); err != nil {
		t.Fatalf("deleting an absent key: %v", err)
	}
}

func TestEntriesExpireWithTheBucketTTL(t *testing.T) {
	_, store := newStore(t, "ttl", time.Second)
	ctx := context.Background()

	if err := store.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// MaxAge is enforced by the server's own timer, so the entry disappears
	// some time after the TTL rather than exactly on it.
	waitUntil(t, 10*time.Second, "the entry to expire", func() bool {
		_, err := store.Get(ctx, "k")
		return errors.Is(err, cache.ErrNotFound)
	})
}

func TestCacheServesItsSecondGetFromTheBucket(t *testing.T) {
	_, store := newStore(t, "cache", time.Minute)
	c := cache.New[string](store)
	ctx := context.Background()

	var calls int32
	loader := func(context.Context) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "loaded", nil
	}

	for i := range 2 {
		got, err := c.Get(ctx, "key", loader)
		if err != nil {
			t.Fatalf("Get #%d: %v", i, err)
		}
		if got != "loaded" {
			t.Fatalf("Get #%d returned %q, want %q", i, got, "loaded")
		}
	}

	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("loader called %d times, want 1", n)
	}
}
