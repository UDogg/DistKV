package server

import (
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/raft"
)

// faults implements network fault injection for demos and testing:
// isolating a node from its peers (a network partition), dropping a
// percentage of Raft messages, or delaying them. Client traffic is not
// affected, so the effect is exactly what a partitioned replica would see:
// its leases lapse, check-quorum demotes it and its shards fail over.
//
// Fault injection is only reachable when Config.EnableFaultInjection is set.
type faults struct {
	isolated    atomic.Bool
	dropPercent atomic.Int32
	delayMs     atomic.Int32
}

var errInjected = errors.New("fault injection: message dropped")

func (f *faults) get() api.Faults {
	return api.Faults{Isolated: f.isolated.Load(), DropPercent: int(f.dropPercent.Load()), DelayMs: int(f.delayMs.Load())}
}

func (f *faults) active() bool {
	return f.isolated.Load() || f.dropPercent.Load() > 0 || f.delayMs.Load() > 0
}

// apply returns errInjected if a peer message should be dropped, after any
// configured delay.
func (f *faults) apply() error {
	if f.isolated.Load() {
		return errInjected
	}
	if d := f.delayMs.Load(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	if p := f.dropPercent.Load(); p > 0 && rand.Int31n(100) < p {
		return errInjected
	}
	return nil
}

// outbound is installed as the Raft transport hook.
func (f *faults) outbound(raft.NodeID) error { return f.apply() }

// inbound wraps the Raft RPC handler.
func (f *faults) inbound(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := f.apply(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		h(w, r)
	}
}

// handleFaults serves GET/POST /v1/admin/faults.
func (s *Server) handleFaults(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req api.Faults
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "invalid JSON: " + err.Error(), Code: api.CodeBadRequest})
			return
		}
		if req.DropPercent < 0 || req.DropPercent > 100 || req.DelayMs < 0 || req.DelayMs > 10_000 {
			writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "drop_percent must be 0-100 and delay_ms 0-10000", Code: api.CodeBadRequest})
			return
		}
		s.faults.isolated.Store(req.Isolated)
		s.faults.dropPercent.Store(int32(req.DropPercent))
		s.faults.delayMs.Store(int32(req.DelayMs))
		s.logger.Warn("fault injection updated", "isolated", req.Isolated, "drop_percent", req.DropPercent, "delay_ms", req.DelayMs)
	}
	writeJSON(w, http.StatusOK, s.faults.get())
}
