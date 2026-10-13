// Package e2e contains end-to-end fault-injection tests that run full
// DistKV clusters (real HTTP, real storage) inside the test process.
//
// They are skipped with -short. Run them with:
//
//	go test -v -run Chaos ./test/e2e/
package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/linearizability"
	"github.com/UDogg/DistKV/internal/loadgen"
	"github.com/UDogg/DistKV/internal/testcluster"
	"github.com/UDogg/DistKV/pkg/client"
)

// nemesis repeatedly injects failures until stop is closed. Only one node
// is down at a time so every shard keeps a quorum (RF=3).
func nemesis(t *testing.T, tc *testcluster.Cluster, stop <-chan struct{}, rng *rand.Rand, log func(string, ...any)) {
	ids := tc.IDs()
	for {
		select {
		case <-stop:
			return
		case <-time.After(time.Duration(300+rng.Intn(500)) * time.Millisecond):
		}
		victim := ids[rng.Intn(len(ids))]
		switch rng.Intn(3) {
		case 0, 1:
			log("nemesis: crashing %s", victim)
			tc.Kill(victim)
		case 2:
			log("nemesis: gracefully restarting %s", victim)
			tc.Shutdown(victim)
		}
		select {
		case <-stop:
		case <-time.After(time.Duration(200+rng.Intn(800)) * time.Millisecond):
		}
		log("nemesis: restarting %s", victim)
		tc.Restart(victim)
	}
}

// TestChaosLinearizability records a concurrent history of puts, gets and
// deletes from several clients while nodes crash and restart, then checks
// that the history is linearizable.
func TestChaosLinearizability(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test")
	}
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewSource(seed))
	tc := testcluster.Start(t, testcluster.Options{
		Nodes: 5, Shards: 6, ReplicationFactor: 3, Persistent: true, SnapshotThreshold: 200,
	})

	const clients, keys = 8, 6
	duration := 12 * time.Second
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	var mu sync.Mutex
	var history []linearizability.Op
	record := func(op linearizability.Op) {
		mu.Lock()
		history = append(history, op)
		mu.Unlock()
	}

	stop := make(chan struct{})
	var nemesisWG sync.WaitGroup
	nemesisWG.Add(1)
	go func() {
		defer nemesisWG.Done()
		nemesis(t, tc, stop, rand.New(rand.NewSource(seed+1)), t.Logf)
	}()

	var wg sync.WaitGroup
	var okOps, failedOps atomic.Int64
	deadline := time.Now().Add(duration)
	for ci := 0; ci < clients; ci++ {
		c := tc.Client()
		crng := rand.New(rand.NewSource(rng.Int63()))
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				key := fmt.Sprintf("k%d", crng.Intn(keys))
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				op := linearizability.Op{ClientID: ci, Key: key, Call: now()}
				switch r := crng.Intn(10); {
				case r < 4:
					op.Kind = linearizability.Put
					op.Value = fmt.Sprintf("c%d-%d", ci, i)
					_, err := c.Put(ctx, key, []byte(op.Value))
					op.Return = now()
					op.Unknown = err != nil
				case r < 9:
					op.Kind = linearizability.Get
					it, err := c.Get(ctx, key)
					op.Return = now()
					switch {
					case err == nil:
						op.Found, op.Value = true, string(it.Value)
					case errors.Is(err, client.ErrNotFound):
					default:
						cancel()
						failedOps.Add(1)
						continue // a failed read has no effect
					}
				default:
					op.Kind = linearizability.Delete
					err := c.Delete(ctx, key)
					op.Return = now()
					switch {
					case err == nil:
						op.Found = true
					case errors.Is(err, client.ErrNotFound):
					default:
						op.Unknown = true
					}
				}
				cancel()
				if op.Unknown {
					failedOps.Add(1)
				} else {
					okOps.Add(1)
				}
				record(op)
			}
		}(ci)
	}
	wg.Wait()
	close(stop)
	nemesisWG.Wait()

	t.Logf("history: %d ops (%d ok, %d failed/indeterminate)", len(history), okOps.Load(), failedOps.Load())
	if okOps.Load() < 500 {
		t.Fatalf("too few successful operations (%d) for a meaningful check", okOps.Load())
	}
	res := linearizability.Check(history, 50_000_000)
	t.Log(res)
	if !res.Ok {
		t.Fatalf("history is not linearizable: %v", res)
	}
}

// TestChaosAvailability drives a steady open-loop workload while nodes are
// crashed and restarted, and requires >= 99.9% of operations to succeed.
// Client retries plus fast Raft failover (pre-vote, check-quorum and
// leadership transfer on graceful shutdown) keep failures invisible.
func TestChaosAvailability(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test")
	}
	seed := time.Now().UnixNano()
	t.Logf("seed %d", seed)
	tc := testcluster.Start(t, testcluster.Options{
		Nodes: 5, Shards: 10, ReplicationFactor: 3, Persistent: true,
		HeartbeatInterval: 50 * time.Millisecond, ElectionTimeout: 300 * time.Millisecond,
	})
	c := tc.Client()
	cfg := loadgen.Config{
		Rate: 1500, Duration: 15 * time.Second, Concurrency: 128, Keys: 2000,
		ValueSize: 64, ReadRatio: 0.8, OpTimeout: 3 * time.Second,
	}
	if err := loadgen.Preload(context.Background(), c, cfg); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		nemesis(t, tc, stop, rand.New(rand.NewSource(seed)), t.Logf)
	}()
	rep := loadgen.Run(context.Background(), c, cfg)
	close(stop)
	<-done

	t.Logf("\n%s", rep.Format())
	if avail := rep.Availability(); avail < 0.999 {
		t.Fatalf("availability %.4f%% < 99.9%%", avail*100)
	}
	tc.WaitHealthy(10 * time.Second)
}

// TestChaosRollingRestart restarts every node in turn (as a deployment
// would). Leadership is transferred before each shutdown, so the workload
// should see no errors at all.
func TestChaosRollingRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test")
	}
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 8, Persistent: true})
	c := tc.Client()
	cfg := loadgen.Config{Rate: 1000, Duration: 10 * time.Second, Concurrency: 64, Keys: 1000, ReadRatio: 0.7}
	done := make(chan *loadgen.Report)
	go func() { done <- loadgen.Run(context.Background(), c, cfg) }()
	time.Sleep(time.Second)
	for _, id := range tc.IDs() {
		tc.Shutdown(id)
		tc.Restart(id)
		waitAllLeaders(t, tc)
		time.Sleep(500 * time.Millisecond)
	}
	rep := <-done
	t.Logf("\n%s", rep.Format())
	if rep.Errors > 0 {
		t.Fatalf("%d operations failed during a rolling restart", rep.Errors)
	}
}

func waitAllLeaders(t *testing.T, tc *testcluster.Cluster) {
	t.Helper()
	tc.WaitHealthy(10 * time.Second)
	_ = cluster.ShardID(0)
}
