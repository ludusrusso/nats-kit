package cache_test

import (
	"context"
	"fmt"
	"time"

	"github.com/ludusrusso/nats-kit/cache"
	"github.com/ludusrusso/nats-kit/cache/natskv"
	"github.com/ludusrusso/nats-kit/natstest"
)

// Example caches a value every replica of a service can share, in a JetStream
// KV bucket that holds it for a minute and then forgets it.
//
// Nothing here says when the entry dies: the bucket does, once, for every key
// it holds. The Cache only decides what happens on a miss — one call to the
// Loader, no matter how many callers arrive at once.
func Example() {
	// An Example has no *testing.T, so it uses StartServer rather than
	// Start. In a real test, natstest.Start(t) is the one you want.
	nc, cleanup, err := natstest.StartServer()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()

	ctx := context.Background()

	// One bucket per Cache: cache_customers here, backed by the
	// KV_cache_customers stream. Swapping this for cache.NewMemory(time.Minute)
	// is the whole difference between a shared cache and a per-process one.
	store, err := natskv.New(ctx, nc, natskv.Config{Bucket: "customers", TTL: time.Minute})
	if err != nil {
		panic(err)
	}

	customers := cache.New[string](store)

	// The Loader is whatever answering the question really costs: a database
	// query, an HTTP call to another service. It runs on a miss, and its
	// result is what gets stored.
	lookups := 0
	lookup := func(context.Context) (string, error) {
		lookups++
		return "ACME Inc.", nil
	}

	for range 3 {
		name, err := customers.Get(ctx, "cust-1", lookup)
		if err != nil {
			panic(err)
		}
		fmt.Println("customer:", name)
	}

	fmt.Println("lookups:", lookups)

	// Output:
	// customer: ACME Inc.
	// customer: ACME Inc.
	// customer: ACME Inc.
	// lookups: 1
}
