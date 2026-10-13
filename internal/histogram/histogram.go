// Package histogram provides a fixed-memory, log-linear latency histogram
// (in the spirit of HdrHistogram) with ~1% relative precision from 1µs to
// ~1 hour, suitable for reporting high percentiles such as p99.9.
package histogram

import (
	"math"
	"math/bits"
	"sync"
	"time"
)

const (
	subBucketBits  = 7 // 128 linear sub-buckets per power of two => <1% error
	subBucketCount = 1 << subBucketBits
	maxExponent    = 42 // values up to 2^42 µs (~50 days)
)

// Histogram records durations. The zero value is not usable; call New.
type Histogram struct {
	mu     sync.Mutex
	counts []uint64
	total  uint64
	min    int64
	max    int64
	sum    float64
}

// New returns an empty histogram.
func New() *Histogram {
	return &Histogram{counts: make([]uint64, numBuckets()), min: math.MaxInt64}
}

// Bucket layout: values below subBucketCount get one bucket each. Above
// that, every power-of-two range [2^k, 2^(k+1)) is split into
// subBucketCount/2 equal-width buckets, giving a relative error < 2/128.
const halfCount = subBucketCount / 2

func numBuckets() int { return subBucketCount + maxExponent*halfCount }

func index(us int64) int {
	if us < subBucketCount {
		return int(us)
	}
	exp := bits.Len64(uint64(us)) - subBucketBits // >= 1
	if exp > maxExponent {
		return numBuckets() - 1
	}
	sub := int(us >> uint(exp)) // in [halfCount, subBucketCount)
	return subBucketCount + (exp-1)*halfCount + sub - halfCount
}

// lowerBound returns the smallest value mapping to bucket i.
func lowerBound(i int) int64 {
	if i < subBucketCount {
		return int64(i)
	}
	j := i - subBucketCount
	exp := j/halfCount + 1
	sub := j%halfCount + halfCount
	return int64(sub) << uint(exp)
}

// Record adds one observation.
func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	i := index(us)
	h.mu.Lock()
	h.counts[i]++
	h.total++
	h.sum += float64(us)
	h.min = min(h.min, us)
	h.max = max(h.max, us)
	h.mu.Unlock()
}

// Merge adds all observations from o.
func (h *Histogram) Merge(o *Histogram) {
	o.mu.Lock()
	counts := append([]uint64(nil), o.counts...)
	total, sum, mn, mx := o.total, o.sum, o.min, o.max
	o.mu.Unlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, c := range counts {
		h.counts[i] += c
	}
	h.total += total
	h.sum += sum
	h.min = min(h.min, mn)
	h.max = max(h.max, mx)
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Quantile returns the value at quantile q in [0, 1].
func (h *Histogram) Quantile(q float64) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	if q >= 1 {
		return time.Duration(h.max) * time.Microsecond
	}
	target := uint64(math.Ceil(q * float64(h.total)))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			v := lowerBound(i)
			v = max(v, h.min)
			v = min(v, h.max)
			return time.Duration(v) * time.Microsecond
		}
	}
	return time.Duration(h.max) * time.Microsecond
}

// Mean returns the arithmetic mean.
func (h *Histogram) Mean() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.sum/float64(h.total)) * time.Microsecond
}

// Max returns the largest observation.
func (h *Histogram) Max() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.max) * time.Microsecond
}
