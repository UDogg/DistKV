// Package api defines DistKV's HTTP wire protocol shared by the server and
// the Go client: header names, error codes and JSON document shapes.
package api

// Request headers.
const (
	// HeaderClientID, HeaderSeq and HeaderAckBelow enable exactly-once
	// semantics for retried writes (see kv.Command).
	HeaderClientID = "X-Distkv-Client-Id"
	HeaderSeq      = "X-Distkv-Seq"
	HeaderAckBelow = "X-Distkv-Ack-Below"
	// HeaderNoForward asks the server to answer 421 with a leader hint
	// instead of proxying to the leader. Smart clients set it so they can
	// learn the topology and talk to leaders directly.
	HeaderNoForward = "X-Distkv-No-Forward"
	// HeaderHops counts server-side forwarding hops (loop protection).
	HeaderHops = "X-Distkv-Hops"
)

// Response headers.
const (
	HeaderShard     = "X-Distkv-Shard"
	HeaderLeader    = "X-Distkv-Leader"
	HeaderVersion   = "X-Distkv-Version"
	HeaderExpiresAt = "X-Distkv-Expires-At"
	HeaderServedBy  = "X-Distkv-Served-By"
)

// Error codes carried in ErrorResponse.Code.
const (
	CodeNotFound      = "not_found"
	CodeConflict      = "conflict"
	CodeNotLeader     = "not_leader"
	CodeNoLeader      = "no_leader"
	CodeUnavailable   = "unavailable"
	CodeTimeout       = "timeout"
	CodeOutcomeUnsure = "outcome_unknown"
	CodeBadRequest    = "bad_request"
	CodeTooLarge      = "too_large"
	CodeStale         = "stale_request"
	CodeShuttingDown  = "shutting_down"
	CodeInternal      = "internal"
)

// Query parameters.
const (
	QueryConsistency = "consistency" // "linearizable" (default) or "stale"
	QueryTTL         = "ttl"         // Go duration, e.g. "30s"
	QueryIfVersion   = "if_version"  // compare-and-set; 0 = create only
)

// Consistency levels for reads.
const (
	Linearizable = "linearizable"
	Stale        = "stale"
)

// ErrorResponse is the body of every non-2xx response.
type ErrorResponse struct {
	Error          string `json:"error"`
	Code           string `json:"code"`
	Shard          *int   `json:"shard,omitempty"`
	Leader         string `json:"leader,omitempty"`
	LeaderAddr     string `json:"leader_addr,omitempty"`
	CurrentVersion uint64 `json:"current_version,omitempty"`
}

// WriteResponse is returned by successful PUT and DELETE requests.
type WriteResponse struct {
	Key     string `json:"key"`
	Version uint64 `json:"version"`
	Shard   int    `json:"shard"`
	Deleted bool   `json:"deleted,omitempty"`
}

// Member describes a server.
type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Spec mirrors cluster.Spec on the wire.
type Spec struct {
	Members           []Member `json:"members"`
	Shards            int      `json:"shards"`
	ReplicationFactor int      `json:"replication_factor"`
	VirtualNodes      int      `json:"virtual_nodes"`
}

// GroupStatus is one node's view of one shard's Raft group.
type GroupStatus struct {
	Shard         int               `json:"shard"`
	Replicas      []string          `json:"replicas"`
	State         string            `json:"state"`
	Term          uint64            `json:"term"`
	Leader        string            `json:"leader,omitempty"`
	CommitIndex   uint64            `json:"commit_index"`
	AppliedIndex  uint64            `json:"applied_index"`
	LastIndex     uint64            `json:"last_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	Keys          int               `json:"keys"`
	Bytes         int64             `json:"bytes"`
	Followers     map[string]Follow `json:"followers,omitempty"`
}

// Follow is a leader's view of a follower.
type Follow struct {
	Match         uint64  `json:"match_index"`
	Lag           uint64  `json:"lag"`
	LastContactMS float64 `json:"last_contact_ms"`
}

// NodeStatus is returned by GET /v1/status.
type NodeStatus struct {
	Node        string        `json:"node"`
	Addr        string        `json:"addr"`
	Version     string        `json:"version"`
	Fingerprint string        `json:"fingerprint"`
	Ready       bool          `json:"ready"`
	UptimeSec   float64       `json:"uptime_seconds"`
	Groups      []GroupStatus `json:"groups"`
}

// ShardSummary is the cluster-wide view of a shard.
type ShardSummary struct {
	Shard       int      `json:"shard"`
	Replicas    []string `json:"replicas"`
	Leader      string   `json:"leader,omitempty"`
	LeaderAddr  string   `json:"leader_addr,omitempty"`
	Term        uint64   `json:"term"`
	CommitIndex uint64   `json:"commit_index"`
	Keys        int      `json:"keys"`
	Healthy     bool     `json:"healthy"`
}

// NodeSummary is the cluster-wide view of a node.
type NodeSummary struct {
	ID         string `json:"id"`
	Addr       string `json:"addr"`
	Reachable  bool   `json:"reachable"`
	Ready      bool   `json:"ready"`
	ShardsLed  int    `json:"shards_led"`
	ShardsHeld int    `json:"shards_hosted"`
	Version    string `json:"version,omitempty"`
}

// ClusterStatus is returned by GET /v1/cluster.
type ClusterStatus struct {
	Spec          Spec           `json:"spec"`
	Fingerprint   string         `json:"fingerprint"`
	Nodes         []NodeSummary  `json:"nodes"`
	Shards        []ShardSummary `json:"shards"`
	HealthyShards int            `json:"healthy_shards"`
}
