package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/linearizability"
	"github.com/UDogg/DistKV/internal/testcluster"
)

func get(key string, found bool, value string, call, ret int64) linearizability.Op {
	return linearizability.Op{Kind: linearizability.Get, Key: key, Found: found, Value: value, Call: call, Return: ret}
}

func put(key, value string, call, ret int64) linearizability.Op {
	return linearizability.Op{Kind: linearizability.Put, Key: key, Value: value, Call: call, Return: ret}
}

func TestSegmentPinnedByBarrierRead(t *testing.T) {
	pin := get("k", true, "a", 10, 20) // the epoch starts from "a"

	ok := segmentHistory(pin, nil, []linearizability.Op{put("k", "b", 30, 40), get("k", true, "b", 50, 60)}, get("k", true, "b", 70, 80))
	if r := linearizability.Check(ok, 0); !r.Ok {
		t.Fatalf("valid epoch rejected: %v", r)
	}

	// A stale read: "a" after the write of "b" completed.
	stale := segmentHistory(pin, nil, []linearizability.Op{put("k", "b", 30, 40), get("k", true, "a", 50, 60)}, get("k", true, "b", 70, 80))
	if r := linearizability.Check(stale, 0); r.Ok {
		t.Fatal("stale read accepted")
	}

	// The pinned value matters: reading "a" without any write is fine only
	// because the barrier read observed "a".
	if r := linearizability.Check(segmentHistory(pin, nil, nil, get("k", true, "a", 30, 40)), 0); !r.Ok {
		t.Fatalf("pinned value not applied: %v", r)
	}
	absent := get("k", false, "", 10, 20)
	if r := linearizability.Check(segmentHistory(absent, nil, nil, get("k", true, "a", 30, 40)), 0); r.Ok {
		t.Fatal("a value appeared from nowhere")
	}
}

func TestCarriedIndeterminateWrite(t *testing.T) {
	// A write from an earlier epoch timed out; it then commits after the
	// barrier that opened this epoch. Without carrying it over, the value
	// would appear from nowhere.
	late := put("k", "x", 5, 6)
	late.Unknown = true
	pin := get("k", true, "a", 10, 20)
	closing := get("k", true, "x", 30, 40)
	if r := linearizability.Check(segmentHistory(pin, []linearizability.Op{late}, nil, closing), 0); !r.Ok {
		t.Fatalf("carried write not considered: %v", r)
	}
	if r := linearizability.Check(segmentHistory(pin, nil, nil, closing), 0); r.Ok {
		t.Fatal("expected a violation without the carried write")
	}
}

func TestProbeUnderChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a cluster")
	}
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 3, ElectionTimeout: 200 * time.Millisecond})
	p := newProbe(tc.Client(), probeOptions{keys: 4, workers: 4, rate: 300, epoch: 400 * time.Millisecond, opTimeout: 2 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runProbe(ctx)
	}()

	time.Sleep(1500 * time.Millisecond)
	victim := tc.IDs()[0]
	tc.Kill(victim)
	time.Sleep(2 * time.Second)
	tc.Restart(victim)
	tc.WaitHealthy(10 * time.Second)
	time.Sleep(1500 * time.Millisecond)
	cancel()
	<-done

	r := p.report()
	if r.Violations == nil || r.Recent == nil {
		t.Fatal("report slices must encode as [], not null")
	}
	t.Logf("probe: verdict=%s epochs=%d ops=%d indeterminate=%d inconclusive=%d", r.Verdict, r.Epochs, r.OpsChecked, r.Indeterminate, r.Inconclusive)
	if r.Verdict != "linearizable" {
		t.Fatalf("verdict %q: %+v", r.Verdict, r.Violations)
	}
	if r.Epochs < 10 || r.OpsChecked < 200 {
		t.Fatalf("too little checked: %d epochs, %d ops", r.Epochs, r.OpsChecked)
	}
}
