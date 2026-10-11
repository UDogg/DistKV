package linearizability

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestSequentialHistory(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Get, Key: "x", Value: "1", Found: true, Call: 2, Return: 3},
		{Kind: Delete, Key: "x", Found: true, Call: 4, Return: 5},
		{Kind: Get, Key: "x", Found: false, Call: 6, Return: 7},
	}
	if r := Check(h, 0); !r.Ok {
		t.Fatal(r)
	}
}

func TestStaleReadDetected(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 0, Return: 1},
		{Kind: Put, Key: "x", Value: "2", Call: 2, Return: 3},
		// Read starts after the second write completed but sees the first.
		{Kind: Get, Key: "x", Value: "1", Found: true, Call: 4, Return: 5},
	}
	if r := Check(h, 0); r.Ok {
		t.Fatal("stale read accepted")
	}
}

func TestConcurrentOverlapAllowsEitherOrder(t *testing.T) {
	h := []Op{
		{ClientID: 1, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10},
		{ClientID: 2, Kind: Put, Key: "x", Value: "b", Call: 1, Return: 9},
		{ClientID: 3, Kind: Get, Key: "x", Value: "a", Found: true, Call: 11, Return: 12},
		{ClientID: 4, Kind: Get, Key: "x", Value: "a", Found: true, Call: 13, Return: 14},
	}
	if r := Check(h, 0); !r.Ok {
		t.Fatal(r)
	}
	// But two sequential reads may not observe a -> b -> a without a write.
	h = append(h, Op{ClientID: 5, Kind: Get, Key: "x", Value: "b", Found: true, Call: 15, Return: 16},
		Op{ClientID: 6, Kind: Get, Key: "x", Value: "a", Found: true, Call: 17, Return: 18})
	if r := Check(h, 0); r.Ok {
		t.Fatal("non-monotonic reads accepted")
	}
}

func TestUnknownOutcomeWrites(t *testing.T) {
	// A timed-out write may have taken effect...
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Unknown: true, Call: 0},
		{Kind: Get, Key: "x", Value: "1", Found: true, Call: 5, Return: 6},
	}
	if r := Check(h, 0); !r.Ok {
		t.Fatal(r)
	}
	// ...or not.
	h[1] = Op{Kind: Get, Key: "x", Found: false, Call: 5, Return: 6}
	if r := Check(h, 0); !r.Ok {
		t.Fatal(r)
	}
	// But once observed it cannot "un-happen".
	h = append(h[:1],
		Op{Kind: Get, Key: "x", Value: "1", Found: true, Call: 5, Return: 6},
		Op{Kind: Get, Key: "x", Found: false, Call: 7, Return: 8})
	if r := Check(h, 0); r.Ok {
		t.Fatal("write that un-happened accepted")
	}
}

func TestIndependentKeys(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "a", Value: "1", Call: 0, Return: 1},
		{Kind: Put, Key: "b", Value: "2", Call: 0, Return: 1},
		{Kind: Get, Key: "a", Value: "1", Found: true, Call: 2, Return: 3},
		{Kind: Get, Key: "b", Value: "1", Found: true, Call: 2, Return: 3},
	}
	r := Check(h, 0)
	if r.Ok || r.FailedKey != "b" {
		t.Fatalf("expected failure on key b, got %v", r)
	}
}

// A real linearizable object (a mutex-protected map) driven concurrently
// must always pass, and the checker must stay fast on large histories.
func TestRandomLinearizableHistories(t *testing.T) {
	for trial := 0; trial < 20; trial++ {
		var mu sync.Mutex
		state := map[string]string{}
		var clock int64
		tick := func() int64 { clock++; return clock }
		var hist []Op
		var hmu sync.Mutex
		var wg sync.WaitGroup
		for c := 0; c < 6; c++ {
			wg.Add(1)
			go func(c int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(trial*100 + c)))
				for i := 0; i < 150; i++ {
					key := fmt.Sprint(rng.Intn(3))
					op := Op{ClientID: c, Key: key}
					mu.Lock()
					op.Call = tick()
					switch rng.Intn(3) {
					case 0:
						op.Kind, op.Value = Put, fmt.Sprintf("%d-%d", c, i)
						state[key] = op.Value
					case 1:
						op.Kind = Get
						op.Value, op.Found = state[key]
					case 2:
						op.Kind = Delete
						_, op.Found = state[key]
						delete(state, key)
					}
					mu.Unlock()
					mu.Lock()
					op.Return = tick()
					mu.Unlock()
					hmu.Lock()
					hist = append(hist, op)
					hmu.Unlock()
				}
			}(c)
		}
		wg.Wait()
		if r := Check(hist, 0); !r.Ok {
			t.Fatalf("trial %d: %v", trial, r)
		}
	}
}
