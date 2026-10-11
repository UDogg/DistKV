// Package linearizability checks whether a concurrent history of
// key-value operations is linearizable.
//
// It implements the Wing & Gong search with Lowe's memoisation (the same
// approach as the Porcupine checker): operations on different keys are
// independent (P-compositionality), so each key's sub-history is checked
// separately against a single-register model.
//
// Operations whose outcome is unknown (the client timed out) are recorded
// with Return = math.MaxInt64: they may take effect at any point after
// their invocation, or never.
package linearizability

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// Kind is an operation type.
type Kind uint8

const (
	Get Kind = iota
	Put
	Delete
)

// Op is one client operation.
type Op struct {
	ClientID int
	Kind     Kind
	Key      string
	// Value is the written value (Put) or the observed value (Get).
	Value string
	// Found is the observed presence for Get, or "deleted something" for
	// Delete.
	Found bool
	// Unknown marks a write whose outcome the client never learned.
	Unknown bool
	Call    int64
	Return  int64
}

// Result describes a check.
type Result struct {
	Ok         bool
	Keys       int
	Ops        int
	FailedKey  string
	FailedOps  []Op
	Incomplete bool // the search was abandoned (budget exhausted)
}

func (r Result) String() string {
	if r.Ok {
		return fmt.Sprintf("linearizable (%d ops over %d keys)", r.Ops, r.Keys)
	}
	if r.Incomplete {
		return fmt.Sprintf("inconclusive: search budget exhausted on key %q", r.FailedKey)
	}
	return fmt.Sprintf("NOT linearizable: key %q (%d ops)", r.FailedKey, len(r.FailedOps))
}

// Check verifies a history. maxSteps bounds the search per key (0 = 10M).
func Check(history []Op, maxSteps int) Result {
	if maxSteps <= 0 {
		maxSteps = 10_000_000
	}
	byKey := map[string][]Op{}
	for _, op := range history {
		if op.Unknown {
			op.Return = math.MaxInt64
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	res := Result{Ok: true, Keys: len(keys), Ops: len(history)}
	for _, k := range keys {
		ok, complete := checkKey(byKey[k], maxSteps)
		if !ok {
			return Result{Ok: false, Keys: len(keys), Ops: len(history), FailedKey: k, FailedOps: byKey[k], Incomplete: !complete}
		}
	}
	return res
}

// register is the sequential model of a single key.
type register struct {
	present bool
	value   string
}

func step(s register, op *Op) (bool, register) {
	switch op.Kind {
	case Get:
		if op.Found != s.present || (op.Found && op.Value != s.value) {
			return false, s
		}
		return true, s
	case Put:
		return true, register{present: true, value: op.Value}
	case Delete:
		if !op.Unknown && op.Found != s.present {
			return false, s
		}
		return true, register{}
	}
	return false, s
}

type entry struct {
	op     int
	isCall bool
	time   int64
	match  *entry // call <-> return
	prev   *entry
	next   *entry
}

func checkKey(ops []Op, maxSteps int) (ok bool, complete bool) {
	n := len(ops)
	events := make([]*entry, 0, 2*n)
	for i := range ops {
		call := &entry{op: i, isCall: true, time: ops[i].Call}
		ret := &entry{op: i, time: ops[i].Return}
		call.match, ret.match = ret, call
		events = append(events, call, ret)
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].time != events[j].time {
			return events[i].time < events[j].time
		}
		// Calls before returns at the same instant: treat as concurrent.
		return events[i].isCall && !events[j].isCall
	})
	head := &entry{} // sentinel
	prev := head
	for _, e := range events {
		prev.next = e
		e.prev = prev
		prev = e
	}

	lift := func(e *entry) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		m := e.match
		m.prev.next = m.next
		if m.next != nil {
			m.next.prev = m.prev
		}
	}
	unlift := func(e *entry) {
		m := e.match
		m.prev.next = m
		if m.next != nil {
			m.next.prev = m
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e     *entry
		state register
	}
	linearized := make([]uint64, (n+63)/64)
	cache := map[string]struct{}{}
	cacheKey := func(s register) string {
		b := make([]byte, 0, len(linearized)*8+len(s.value)+1)
		for _, w := range linearized {
			b = binary.LittleEndian.AppendUint64(b, w)
		}
		if s.present {
			b = append(b, 1)
			b = append(b, s.value...)
		} else {
			b = append(b, 0)
		}
		return string(b)
	}

	var stack []frame
	state := register{}
	e := head.next
	steps := 0
	for head.next != nil {
		if steps++; steps > maxSteps {
			return false, false
		}
		if e == nil {
			// Only reachable if every remaining entry is a call that
			// cannot be linearized; backtrack.
			if len(stack) == 0 {
				return false, true
			}
			f := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			state = f.state
			linearized[f.e.op/64] &^= 1 << (f.e.op % 64)
			unlift(f.e)
			e = f.e.next
			continue
		}
		if e.isCall {
			ok, next := step(state, &ops[e.op])
			if ok {
				linearized[e.op/64] |= 1 << (e.op % 64)
				k := cacheKey(next)
				if _, seen := cache[k]; !seen {
					cache[k] = struct{}{}
					stack = append(stack, frame{e: e, state: state})
					state = next
					lift(e)
					e = head.next
					continue
				}
				linearized[e.op/64] &^= 1 << (e.op % 64)
			}
			e = e.next
			continue
		}
		// A return event: the operation must already have been
		// linearized. Backtrack.
		if len(stack) == 0 {
			return false, true
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		linearized[f.e.op/64] &^= 1 << (f.e.op % 64)
		unlift(f.e)
		e = f.e.next
	}
	return true, true
}
