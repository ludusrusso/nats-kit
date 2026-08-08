package cache_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ludusrusso/nats-kit/cache"
)

// token is a value whose own expiry can precede the substrate's TTL, which is
// what WithHitValidator exists for.
type token struct {
	JWT       string    `json:"jwt"`
	ExpiresAt time.Time `json:"expires_at"`
}

// loaderReturning counts its calls, which is how every test below tells a hit
// from a miss.
func loaderReturning(v token, calls *int32) cache.Loader[token] {
	return func(context.Context) (token, error) {
		atomic.AddInt32(calls, 1)
		return v, nil
	}
}

func TestGetLoadsOnMissThenServesTheHit(t *testing.T) {
	ctx := context.Background()
	c := cache.New[token](cache.NewMemory(time.Minute))

	want := token{JWT: "tok-1"}
	var calls int32

	got, err := c.Get(ctx, "k", loaderReturning(want, &calls))
	if err != nil || got.JWT != want.JWT {
		t.Fatalf("miss: got %v, err %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("miss called the loader %d times, want 1", calls)
	}

	got, err = c.Get(ctx, "k", loaderReturning(want, &calls))
	if err != nil || got.JWT != want.JWT {
		t.Fatalf("hit: got %v, err %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("hit called the loader again: %d calls total", calls)
	}
}

func TestGetReloadsOnceTheSubstrateTTLElapsed(t *testing.T) {
	ctx := context.Background()
	c := cache.New[token](cache.NewMemory(50 * time.Millisecond))

	var calls int32
	loader := loaderReturning(token{JWT: "tok"}, &calls)

	if _, err := c.Get(ctx, "k", loader); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	time.Sleep(70 * time.Millisecond)
	if _, err := c.Get(ctx, "k", loader); err != nil {
		t.Fatalf("second Get: %v", err)
	}

	if calls != 2 {
		t.Fatalf("loader called %d times, want 2 — the entry should have expired", calls)
	}
}

func TestGetCollapsesConcurrentMissesIntoOneLoad(t *testing.T) {
	ctx := context.Background()
	c := cache.New[token](cache.NewMemory(time.Minute))

	var calls int32
	slow := func(context.Context) (token, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		return token{JWT: "tok"}, nil
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Get(ctx, "k", slow); err != nil {
				t.Errorf("Get: %v", err)
			}
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Fatalf("loader called %d times, want 1", calls)
	}
}

func TestGetServesFromTheLoaderWhileTheSubstrateIsUnreachable(t *testing.T) {
	ctx := context.Background()
	kv := cache.NewMemory(time.Minute)
	kv.SetFailure(errors.New("broker unreachable"))
	c := cache.New[token](kv)

	var calls int32
	loader := loaderReturning(token{JWT: "tok"}, &calls)

	got, err := c.Get(ctx, "k", loader)
	if err != nil {
		t.Fatalf("an unreachable substrate must not fail the call: %v", err)
	}
	if got.JWT != "tok" {
		t.Fatalf("got %v, want the loader's value", got)
	}

	if _, err := c.Get(ctx, "k", loader); err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if calls != 2 {
		t.Fatalf("loader called %d times, want 2 — nothing is cached while the substrate is down", calls)
	}
}

func TestGetRoundTripsTheValueThroughJSON(t *testing.T) {
	ctx := context.Background()
	c := cache.New[token](cache.NewMemory(time.Minute))

	expiry := time.Now().Add(10 * time.Minute).Round(time.Second)
	want := token{JWT: "tok-roundtrip", ExpiresAt: expiry}
	var calls int32

	if _, err := c.Get(ctx, "k", loaderReturning(want, &calls)); err != nil {
		t.Fatalf("miss: %v", err)
	}

	got, err := c.Get(ctx, "k", loaderReturning(want, &calls))
	if err != nil {
		t.Fatalf("hit: %v", err)
	}
	if got.JWT != want.JWT || !got.ExpiresAt.Equal(expiry) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
}

func TestGetTreatsAHitTheValidatorRefusesAsAMiss(t *testing.T) {
	ctx := context.Background()
	c := cache.New(cache.NewMemory(time.Minute), cache.WithHitValidator(func(v token) bool {
		return v.JWT != "stale"
	}))

	var calls int32
	if _, err := c.Get(ctx, "k", loaderReturning(token{JWT: "stale"}, &calls)); err != nil {
		t.Fatalf("seeding Get: %v", err)
	}

	got, err := c.Get(ctx, "k", loaderReturning(token{JWT: "fresh"}, &calls))
	if err != nil || got.JWT != "fresh" {
		t.Fatalf("refused hit: got %v, err %v", got, err)
	}
	if calls != 2 {
		t.Fatalf("loader called %d times, want 2", calls)
	}

	got, err = c.Get(ctx, "k", loaderReturning(token{JWT: "unused"}, &calls))
	if err != nil || got.JWT != "fresh" {
		t.Fatalf("the reload must have overwritten the refused entry: got %v, err %v", got, err)
	}
	if calls != 2 {
		t.Fatalf("loader called %d times, want 2", calls)
	}
}

func TestGetNeverCachesALoaderError(t *testing.T) {
	ctx := context.Background()
	c := cache.New[token](cache.NewMemory(time.Minute))

	boom := errors.New("backend down")
	var calls int32
	failing := func(context.Context) (token, error) {
		atomic.AddInt32(&calls, 1)
		return token{}, boom
	}

	for range 2 {
		if _, err := c.Get(ctx, "k", failing); !errors.Is(err, boom) {
			t.Fatalf("got %v, want the loader's error", err)
		}
	}
	if calls != 2 {
		t.Fatalf("loader called %d times, want 2", calls)
	}
}

func TestNewMemoryRejectsAMissingTTL(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewMemory(0) returned instead of panicking")
		}
	}()
	cache.NewMemory(0)
}
