package client_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/testcluster"
	"github.com/UDogg/DistKV/pkg/client"
)

// Example demonstrates basic usage against a running cluster.
func Example() {
	ctx := context.Background()
	c, err := client.New(ctx, client.Config{
		Endpoints: []string{"http://127.0.0.1:7001", "http://127.0.0.1:7002"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Cache a value for ten minutes.
	if _, err := c.Put(ctx, "session:abc", []byte(`{"user":42}`), client.WithTTL(10*time.Minute)); err != nil {
		log.Fatal(err)
	}

	// Linearizable read (served by the shard leader).
	item, err := c.Get(ctx, "session:abc")
	if errors.Is(err, client.ErrNotFound) {
		fmt.Println("miss")
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s (version %d)\n", item.Value, item.Version)

	// Optimistic concurrency: only succeeds if nobody wrote in between.
	_, err = c.CompareAndSwap(ctx, "session:abc", item.Version, []byte(`{"user":43}`))
	var conflict *client.ConflictError
	if errors.As(err, &conflict) {
		fmt.Println("lost the race; current version is", conflict.CurrentVersion)
	}
}

// TestExampleFlow runs the same flow as Example against an in-process
// cluster so the documented API stays honest.
func TestExampleFlow(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	ctx := context.Background()
	c, err := client.New(ctx, client.Config{Endpoints: tc.Endpoints()[:2]})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Put(ctx, "session:abc", []byte(`{"user":42}`), client.WithTTL(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	item, err := c.Get(ctx, "session:abc")
	if err != nil {
		t.Fatal(err)
	}
	if item.ExpiresAt.IsZero() || time.Until(item.ExpiresAt) > 10*time.Minute {
		t.Fatalf("unexpected expiry %v", item.ExpiresAt)
	}
	if _, err := c.CompareAndSwap(ctx, "session:abc", item.Version, []byte(`{"user":43}`)); err != nil {
		t.Fatal(err)
	}
	_, err = c.CompareAndSwap(ctx, "session:abc", item.Version, []byte(`{"user":44}`))
	var conflict *client.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected a conflict, got %v", err)
	}
}
