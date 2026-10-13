package kv

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/raft"
)

type harness struct {
	t     *testing.T
	s     *Store
	index uint64
	now   int64
}

func newHarness(t *testing.T, opts Options) *harness {
	return &harness{t: t, s: NewStore(opts), now: time.Unix(1_700_000_000, 0).UnixNano()}
}

func (h *harness) apply(c Command) Result {
	h.index++
	if c.Now == 0 {
		c.Now = h.now
	}
	return h.s.Apply(raft.Entry{Index: h.index, Term: 1, Data: c.Encode()}).(Result)
}

func (h *harness) get(key string) (Item, bool) { return h.s.Get(key, h.now) }

func TestCommandRoundTrip(t *testing.T) {
	cases := []Command{
		{Op: OpPut, Key: "k", Value: []byte("v"), Now: 42},
		{Op: OpCAS, Key: "", Value: nil, ExpectVersion: 7, ExpireAt: -1},
		{Op: OpDelete, Key: "ключ", ClientID: 1 << 60, Seq: 99, AckBelow: 98},
		{Op: OpPut, Key: "big", Value: bytes.Repeat([]byte{0xAB}, 1<<16)},
	}
	for _, c := range cases {
		got, err := DecodeCommand(c.Encode())
		if err != nil {
			t.Fatalf("decode %+v: %v", c, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, c)
		}
	}
	if _, err := DecodeCommand([]byte{1}); err == nil {
		t.Fatalf("expected error for truncated command")
	}
	enc := cases[0].Encode()
	if _, err := DecodeCommand(enc[:len(enc)-1]); err == nil {
		t.Fatalf("expected error for truncated value")
	}
}

func TestPutGetDelete(t *testing.T) {
	h := newHarness(t, Options{})
	r := h.apply(Command{Op: OpPut, Key: "a", Value: []byte("1")})
	if r.Code != CodeOK || r.Version != 1 {
		t.Fatalf("put: %+v", r)
	}
	it, ok := h.get("a")
	if !ok || string(it.Value) != "1" || it.Version != 1 {
		t.Fatalf("get: %+v %v", it, ok)
	}
	h.apply(Command{Op: OpPut, Key: "a", Value: []byte("2")})
	if it, _ := h.get("a"); string(it.Value) != "2" || it.Version != 2 {
		t.Fatalf("overwrite: %+v", it)
	}
	if r := h.apply(Command{Op: OpDelete, Key: "a"}); r.Code != CodeOK {
		t.Fatalf("delete: %+v", r)
	}
	if _, ok := h.get("a"); ok {
		t.Fatalf("deleted key still visible")
	}
	if r := h.apply(Command{Op: OpDelete, Key: "a"}); r.Code != CodeNotFound {
		t.Fatalf("delete missing: %+v", r)
	}
	if st := h.s.Stats(); st.Keys != 0 || st.Bytes != 0 {
		t.Fatalf("accounting leak: %+v", st)
	}
}

func TestCompareAndSet(t *testing.T) {
	h := newHarness(t, Options{})
	// Create-if-absent.
	if r := h.apply(Command{Op: OpCAS, Key: "k", Value: []byte("a"), ExpectVersion: 0}); r.Code != CodeOK {
		t.Fatalf("create: %+v", r)
	}
	if r := h.apply(Command{Op: OpCAS, Key: "k", Value: []byte("b"), ExpectVersion: 0}); r.Code != CodeConflict || r.Version != 1 {
		t.Fatalf("second create should conflict: %+v", r)
	}
	if r := h.apply(Command{Op: OpCAS, Key: "k", Value: []byte("c"), ExpectVersion: 1}); r.Code != CodeOK || r.Version != 3 {
		t.Fatalf("cas: %+v", r)
	}
	if r := h.apply(Command{Op: OpCAS, Key: "k", Value: []byte("d"), ExpectVersion: 1}); r.Code != CodeConflict || r.Version != 3 {
		t.Fatalf("stale cas: %+v", r)
	}
	if it, _ := h.get("k"); string(it.Value) != "c" {
		t.Fatalf("value = %q", it.Value)
	}
}

func TestTTLExpiry(t *testing.T) {
	h := newHarness(t, Options{})
	h.apply(Command{Op: OpPut, Key: "short", Value: []byte("x"), ExpireAt: h.now + int64(time.Second)})
	h.apply(Command{Op: OpPut, Key: "long", Value: []byte("y"), ExpireAt: h.now + int64(time.Hour)})
	h.apply(Command{Op: OpPut, Key: "forever", Value: []byte("z")})

	// Reads use the caller's wall clock.
	if _, ok := h.s.Get("short", h.now+int64(2*time.Second)); ok {
		t.Fatalf("expired key visible to reader")
	}
	if _, ok := h.s.Get("short", h.now); !ok {
		t.Fatalf("live key not visible")
	}
	// Physical removal happens when the replicated clock advances.
	h.now += int64(2 * time.Second)
	h.apply(Command{Op: OpNoop})
	st := h.s.Stats()
	if st.Keys != 2 || st.Expirations != 1 {
		t.Fatalf("after expiry: %+v", st)
	}
	// Overwriting a key clears its TTL.
	h.apply(Command{Op: OpPut, Key: "long", Value: []byte("y2")})
	h.now += int64(2 * time.Hour)
	h.apply(Command{Op: OpNoop})
	if _, ok := h.get("long"); !ok {
		t.Fatalf("overwrite did not clear TTL")
	}
}

func TestLRUEviction(t *testing.T) {
	h := newHarness(t, Options{MaxKeys: 3})
	for _, k := range []string{"a", "b", "c"} {
		h.apply(Command{Op: OpPut, Key: k, Value: []byte(k)})
	}
	h.apply(Command{Op: OpPut, Key: "a", Value: []byte("a2")}) // a is now most recent
	h.apply(Command{Op: OpPut, Key: "d", Value: []byte("d")})  // evicts b
	if _, ok := h.get("b"); ok {
		t.Fatalf("expected b to be evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := h.get(k); !ok {
			t.Fatalf("%s unexpectedly evicted", k)
		}
	}
	if h.s.Stats().Evictions != 1 {
		t.Fatalf("evictions = %d", h.s.Stats().Evictions)
	}
}

func TestByteCapacity(t *testing.T) {
	h := newHarness(t, Options{MaxBytes: 1000})
	for i := 0; i < 100; i++ {
		h.apply(Command{Op: OpPut, Key: fmt.Sprintf("k%03d", i), Value: make([]byte, 100)})
	}
	if st := h.s.Stats(); st.Bytes > 1000 || st.Keys == 0 {
		t.Fatalf("byte cap not enforced: %+v", st)
	}
	if _, ok := h.get("k099"); !ok {
		t.Fatalf("most recent key evicted")
	}
}

func TestExactlyOnceSessions(t *testing.T) {
	h := newHarness(t, Options{})
	put := Command{Op: OpCAS, Key: "k", Value: []byte("v"), ExpectVersion: 0, ClientID: 7, Seq: 1, AckBelow: 1}
	r1 := h.apply(put)
	// A retry of the same request (e.g. after a leader failover) must
	// return the original result rather than a CAS conflict.
	r2 := h.apply(put)
	if r1 != r2 || r1.Code != CodeOK {
		t.Fatalf("retry not deduplicated: %+v vs %+v", r1, r2)
	}
	if h.s.Stats().DedupHits != 1 {
		t.Fatalf("dedup hits = %d", h.s.Stats().DedupHits)
	}
	// Out-of-order concurrent sequences are fine.
	h.apply(Command{Op: OpPut, Key: "x", ClientID: 7, Seq: 3, AckBelow: 2})
	if r := h.apply(Command{Op: OpPut, Key: "y", ClientID: 7, Seq: 2, AckBelow: 2}); r.Code != CodeOK {
		t.Fatalf("seq 2 after 3: %+v", r)
	}
	// Once acknowledged, an ancient duplicate is rejected as stale.
	h.apply(Command{Op: OpPut, Key: "z", ClientID: 7, Seq: 4, AckBelow: 4})
	if r := h.apply(put); r.Code != CodeStale {
		t.Fatalf("acknowledged duplicate: %+v", r)
	}
}

func TestSessionExpiry(t *testing.T) {
	h := newHarness(t, Options{SessionTTL: time.Minute})
	h.apply(Command{Op: OpPut, Key: "a", ClientID: 1, Seq: 1})
	h.now += int64(time.Hour)
	for i := 0; i < 1100; i++ {
		h.apply(Command{Op: OpNoop})
	}
	if n := h.s.Stats().Sessions; n != 0 {
		t.Fatalf("idle session not expired: %d", n)
	}
}

// Replicas fed the same log must end in identical states, and a replica
// restored from a snapshot must continue identically.
func TestDeterminismAndSnapshot(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	opts := Options{MaxKeys: 50, MaxBytes: 4000}
	a, b := NewStore(opts), NewStore(opts)
	now := time.Unix(1_700_000_000, 0).UnixNano()
	var log []raft.Entry
	for i := 1; i <= 3000; i++ {
		now += int64(rng.Intn(int(10 * time.Millisecond)))
		c := Command{Key: fmt.Sprintf("k%d", rng.Intn(80)), Now: now, ClientID: uint64(rng.Intn(4)), Seq: uint64(i)}
		switch rng.Intn(4) {
		case 0, 1:
			c.Op = OpPut
			c.Value = make([]byte, rng.Intn(64))
			if rng.Intn(3) == 0 {
				c.ExpireAt = now + int64(rng.Intn(int(time.Second)))
			}
		case 2:
			c.Op = OpCAS
			c.ExpectVersion = uint64(rng.Intn(i))
			c.Value = []byte("cas")
		case 3:
			c.Op = OpDelete
		}
		log = append(log, raft.Entry{Index: uint64(i), Term: 1, Data: c.Encode()})
	}
	for _, e := range log[:1500] {
		ra, rb := a.Apply(e), b.Apply(e)
		if ra != rb {
			t.Fatalf("divergent results at %d: %+v vs %+v", e.Index, ra, rb)
		}
	}
	snap, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	c := NewStore(opts)
	if err := c.Restore(snap); err != nil {
		t.Fatal(err)
	}
	for _, e := range log[1500:] {
		ra, rb, rc := a.Apply(e), b.Apply(e), c.Apply(e)
		if ra != rb || ra != rc {
			t.Fatalf("divergent results at %d: %+v / %+v / %+v", e.Index, ra, rb, rc)
		}
	}
	sa, _ := a.Snapshot()
	sb, _ := b.Snapshot()
	sc, _ := c.Snapshot()
	if !bytes.Equal(sa, sb) || !bytes.Equal(sa, sc) {
		t.Fatalf("replica states diverged")
	}
}

func BenchmarkApplyPut(b *testing.B) {
	s := NewStore(Options{MaxKeys: 100_000})
	val := make([]byte, 128)
	entries := make([]raft.Entry, 1024)
	for i := range entries {
		c := Command{Op: OpPut, Key: fmt.Sprintf("key-%d", i), Value: val, Now: int64(i)}
		entries[i] = raft.Entry{Index: uint64(i + 1), Data: c.Encode()}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Apply(entries[i%len(entries)])
	}
}

func BenchmarkGet(b *testing.B) {
	s := NewStore(Options{})
	for i := 0; i < 1024; i++ {
		c := Command{Op: OpPut, Key: fmt.Sprintf("key-%d", i), Value: make([]byte, 128)}
		s.Apply(raft.Entry{Index: uint64(i + 1), Data: c.Encode()})
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key-%d", i%1024), 0)
			i++
		}
	})
}
