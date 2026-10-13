// Package metrics is a small, dependency-free implementation of the
// Prometheus text exposition format (version 0.0.4): counters, gauges and
// histograms with labels, plus scrape-time collectors.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families.
type Registry struct {
	mu         sync.Mutex
	families   []family
	collectors []func(w *Writer)
}

type family interface {
	name() string
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.families {
		if e.name() == f.name() {
			panic("metrics: duplicate metric " + f.name())
		}
	}
	r.families = append(r.families, f)
}

// RegisterCollector adds a function invoked on every scrape to emit
// dynamically computed metrics (e.g. Raft state per shard).
func (r *Registry) RegisterCollector(fn func(w *Writer)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, fn)
}

// Write renders all metrics in the text exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	cols := append([]func(*Writer){}, r.collectors...)
	r.mu.Unlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name() < fams[j].name() })
	for _, f := range fams {
		f.write(w)
	}
	cw := &Writer{w: w, seen: map[string]bool{}}
	for _, c := range cols {
		c(cw)
	}
}

// Handler serves the registry over HTTP.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

// Writer is passed to collectors.
type Writer struct {
	w    io.Writer
	seen map[string]bool
}

// Gauge emits a gauge sample. HELP/TYPE lines are written once per name.
func (cw *Writer) Gauge(name, help string, value float64, labels ...string) {
	cw.sample(name, help, "gauge", value, labels)
}

// Counter emits a counter sample.
func (cw *Writer) Counter(name, help string, value float64, labels ...string) {
	cw.sample(name, help, "counter", value, labels)
}

func (cw *Writer) sample(name, help, typ string, value float64, labels []string) {
	if !cw.seen[name] {
		cw.seen[name] = true
		fmt.Fprintf(cw.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	fmt.Fprintf(cw.w, "%s%s %s\n", name, formatLabels(labels), formatFloat(value))
}

// ---------------------------------------------------------------------------
// Counter

// CounterVec is a set of counters partitioned by label values.
type CounterVec struct {
	n, help string
	labels  []string
	m       sync.Map // label key -> *Counter
}

// Counter is a monotonically increasing value.
type Counter struct {
	labelPairs []string
	v          atomic.Uint64 // float64 bits
}

// NewCounterVec registers a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{n: name, help: help, labels: labels}
	r.register(c)
	return c
}

// With returns the counter for the given label values.
func (c *CounterVec) With(values ...string) *Counter {
	key := strings.Join(values, "\xff")
	if v, ok := c.m.Load(key); ok {
		return v.(*Counter)
	}
	v, _ := c.m.LoadOrStore(key, &Counter{labelPairs: pairs(c.labels, values)})
	return v.(*Counter)
}

// Inc adds one.
func (c *Counter) Inc() { c.Add(1) }

// Add adds delta (must be non-negative).
func (c *Counter) Add(delta float64) {
	for {
		old := c.v.Load()
		nv := math.Float64bits(math.Float64frombits(old) + delta)
		if c.v.CompareAndSwap(old, nv) {
			return
		}
	}
}

// Value returns the current value.
func (c *Counter) Value() float64 { return math.Float64frombits(c.v.Load()) }

func (c *CounterVec) name() string { return c.n }

func (c *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.n, c.help, c.n)
	for _, ctr := range sortedValues[*Counter](&c.m) {
		fmt.Fprintf(w, "%s%s %s\n", c.n, formatLabels(ctr.labelPairs), formatFloat(ctr.Value()))
	}
}

// ---------------------------------------------------------------------------
// Gauge

// GaugeVec is a set of gauges partitioned by label values.
type GaugeVec struct {
	n, help string
	labels  []string
	m       sync.Map
}

// Gauge is a value that can go up and down.
type Gauge struct {
	labelPairs []string
	v          atomic.Uint64
}

// NewGaugeVec registers a gauge family.
func (r *Registry) NewGaugeVec(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{n: name, help: help, labels: labels}
	r.register(g)
	return g
}

// With returns the gauge for the given label values.
func (g *GaugeVec) With(values ...string) *Gauge {
	key := strings.Join(values, "\xff")
	if v, ok := g.m.Load(key); ok {
		return v.(*Gauge)
	}
	v, _ := g.m.LoadOrStore(key, &Gauge{labelPairs: pairs(g.labels, values)})
	return v.(*Gauge)
}

// Set sets the value.
func (g *Gauge) Set(v float64) { g.v.Store(math.Float64bits(v)) }

// Add adds delta.
func (g *Gauge) Add(delta float64) {
	for {
		old := g.v.Load()
		nv := math.Float64bits(math.Float64frombits(old) + delta)
		if g.v.CompareAndSwap(old, nv) {
			return
		}
	}
}

// Value returns the current value.
func (g *Gauge) Value() float64 { return math.Float64frombits(g.v.Load()) }

func (g *GaugeVec) name() string { return g.n }

func (g *GaugeVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.n, g.help, g.n)
	for _, gg := range sortedValues[*Gauge](&g.m) {
		fmt.Fprintf(w, "%s%s %s\n", g.n, formatLabels(gg.labelPairs), formatFloat(gg.Value()))
	}
}

// ---------------------------------------------------------------------------
// Histogram

// DefaultLatencyBuckets are tuned for sub-10ms request latencies.
var DefaultLatencyBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
}

// HistogramVec is a set of histograms partitioned by label values.
type HistogramVec struct {
	n, help string
	labels  []string
	buckets []float64
	m       sync.Map
}

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	labelPairs []string
	buckets    []float64
	counts     []atomic.Uint64
	count      atomic.Uint64
	sum        atomic.Uint64 // float64 bits
}

// NewHistogramVec registers a histogram family.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if buckets == nil {
		buckets = DefaultLatencyBuckets
	}
	h := &HistogramVec{n: name, help: help, labels: labels, buckets: buckets}
	r.register(h)
	return h
}

// With returns the histogram for the given label values.
func (h *HistogramVec) With(values ...string) *Histogram {
	key := strings.Join(values, "\xff")
	if v, ok := h.m.Load(key); ok {
		return v.(*Histogram)
	}
	v, _ := h.m.LoadOrStore(key, &Histogram{
		labelPairs: pairs(h.labels, values),
		buckets:    h.buckets,
		counts:     make([]atomic.Uint64, len(h.buckets)),
	})
	return v.(*Histogram)
}

// Observe records a value (seconds for latency histograms).
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.buckets, v)
	if i < len(h.counts) {
		h.counts[i].Add(1)
	}
	h.count.Add(1)
	for {
		old := h.sum.Load()
		nv := math.Float64bits(math.Float64frombits(old) + v)
		if h.sum.CompareAndSwap(old, nv) {
			return
		}
	}
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

func (h *HistogramVec) name() string { return h.n }

func (h *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.n, h.help, h.n)
	for _, hh := range sortedValues[*Histogram](&h.m) {
		var cum uint64
		for i, b := range hh.buckets {
			cum += hh.counts[i].Load()
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.n, formatLabels(append(append([]string(nil), hh.labelPairs...), "le", formatFloat(b))), cum)
		}
		total := hh.count.Load()
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.n, formatLabels(append(append([]string(nil), hh.labelPairs...), "le", "+Inf")), total)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.n, formatLabels(hh.labelPairs), formatFloat(math.Float64frombits(hh.sum.Load())))
		fmt.Fprintf(w, "%s_count%s %d\n", h.n, formatLabels(hh.labelPairs), total)
	}
}

// ---------------------------------------------------------------------------
// helpers

type labeled interface{ pairsOf() []string }

func (c *Counter) pairsOf() []string   { return c.labelPairs }
func (g *Gauge) pairsOf() []string     { return g.labelPairs }
func (h *Histogram) pairsOf() []string { return h.labelPairs }

func sortedValues[T labeled](m *sync.Map) []T {
	var out []T
	m.Range(func(_, v any) bool {
		out = append(out, v.(T))
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		return strings.Join(out[i].pairsOf(), ",") < strings.Join(out[j].pairsOf(), ",")
	})
	return out
}

func pairs(names, values []string) []string {
	if len(names) != len(values) {
		panic(fmt.Sprintf("metrics: expected %d label values, got %d", len(names), len(values)))
	}
	out := make([]string, 0, 2*len(names))
	for i := range names {
		out = append(out, names[i], values[i])
	}
	return out
}

func formatLabels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(kv[i])
		b.WriteString(`="`)
		b.WriteString(escape(kv[i+1]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func escape(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return strings.ReplaceAll(s, "\n", `\n`)
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
