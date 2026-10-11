package histogram

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestQuantilesAccurate(t *testing.T) {
	h := New()
	rng := rand.New(rand.NewSource(1))
	var vals []time.Duration
	for i := 0; i < 100_000; i++ {
		// Log-normal-ish latency distribution between ~50µs and ~1s.
		d := time.Duration(50+rng.ExpFloat64()*2000) * time.Microsecond
		if rng.Intn(1000) == 0 {
			d *= 100
		}
		vals = append(vals, d)
		h.Record(d)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999} {
		exact := vals[int(q*float64(len(vals)))-1]
		got := h.Quantile(q)
		if rel := float64(got-exact) / float64(exact); rel > 0.02 || rel < -0.02 {
			t.Errorf("q%.3f: got %v, exact %v (%.2f%% error)", q, got, exact, rel*100)
		}
	}
	if h.Max() != vals[len(vals)-1].Truncate(time.Microsecond) {
		t.Errorf("max = %v, want %v", h.Max(), vals[len(vals)-1])
	}
}

func TestIndexMonotonic(t *testing.T) {
	prev := -1
	for us := int64(0); us < 1<<22; us += 1 + us/64 {
		i := index(us)
		if i < prev {
			t.Fatalf("index not monotonic at %d: %d < %d", us, i, prev)
		}
		if lb := lowerBound(i); lb > us {
			t.Fatalf("lowerBound(%d)=%d > %d", i, lb, us)
		}
		prev = i
	}
}

func TestMerge(t *testing.T) {
	a, b := New(), New()
	a.Record(time.Millisecond)
	b.Record(3 * time.Millisecond)
	a.Merge(b)
	if a.Count() != 2 || a.Max() != 3*time.Millisecond || a.Mean() != 2*time.Millisecond {
		t.Fatalf("merge: count=%d max=%v mean=%v", a.Count(), a.Max(), a.Mean())
	}
}
