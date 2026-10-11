package loadgen_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/loadgen"
	"github.com/UDogg/DistKV/internal/testcluster"
)

func TestOpenLoopRateAndReport(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	c := tc.Client()
	cfg := loadgen.Config{Rate: 500, Duration: 2 * time.Second, Warmup: 500 * time.Millisecond,
		Concurrency: 32, Keys: 100, ReadRatio: 0.5}
	if err := loadgen.Preload(context.Background(), c, cfg); err != nil {
		t.Fatal(err)
	}
	var seconds int
	cfg.OnSecond = func(loadgen.Second) { seconds++ }
	rep := loadgen.Run(context.Background(), c, cfg)

	// Open loop: the offered load is fixed by the schedule.
	if rep.Ops < 900 || rep.Ops > 1100 {
		t.Fatalf("ops = %d, want ~1000 (500/s for 2s)", rep.Ops)
	}
	if rep.Errors != 0 || rep.Availability() != 1 {
		t.Fatalf("errors = %d: %v", rep.Errors, rep.ErrorSamples)
	}
	if rep.Reads.Count() == 0 || rep.Writes.Count() == 0 {
		t.Fatalf("expected both reads and writes")
	}
	if seconds < 1 {
		t.Fatalf("no per-second progress reported")
	}
	if out := rep.Format(); !strings.Contains(out, "availability 100.0000%") {
		t.Fatalf("unexpected report:\n%s", out)
	}
}
