package transport

import (
	"reflect"
	"testing"

	"github.com/UDogg/DistKV/internal/raft"
)

func TestCodecRoundTrip(t *testing.T) {
	msgs := []struct{ in, out any }{
		{&raft.RequestVoteRequest{Term: 7, CandidateID: "n2", LastLogIndex: 99, LastLogTerm: 6, PreVote: true, LeadershipTransfer: true}, &raft.RequestVoteRequest{}},
		{&raft.RequestVoteResponse{Term: 7, Granted: true}, &raft.RequestVoteResponse{}},
		{&raft.AppendEntriesRequest{Term: 3, LeaderID: "n1", PrevLogIndex: 10, PrevLogTerm: 2, LeaderCommit: 9,
			Entries: []raft.Entry{{Index: 11, Term: 3, Type: raft.EntryNoop}, {Index: 12, Term: 3, Data: []byte("payload")}}}, &raft.AppendEntriesRequest{}},
		{&raft.AppendEntriesRequest{Term: 3, LeaderID: "n1"}, &raft.AppendEntriesRequest{}},
		{&raft.AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 12, ConflictTerm: 1, ConflictIndex: 4}, &raft.AppendEntriesResponse{}},
		{&raft.InstallSnapshotRequest{Term: 5, LeaderID: "n3", Meta: raft.SnapshotMeta{Index: 1000, Term: 4}, Data: []byte("snapshot")}, &raft.InstallSnapshotRequest{}},
		{&raft.InstallSnapshotResponse{Term: 5}, &raft.InstallSnapshotResponse{}},
		{&raft.TimeoutNowRequest{Term: 8, LeaderID: "n1"}, &raft.TimeoutNowRequest{}},
		{&raft.TimeoutNowResponse{Term: 9}, &raft.TimeoutNowResponse{}},
	}
	for _, m := range msgs {
		b, err := marshal(nil, m.in)
		if err != nil {
			t.Fatal(err)
		}
		if err := unmarshal(b, m.out); err != nil {
			t.Fatalf("%T: %v", m.in, err)
		}
		if !reflect.DeepEqual(m.in, m.out) {
			t.Fatalf("%T round trip:\n got %+v\nwant %+v", m.in, m.out, m.in)
		}
		// Every strict prefix must be rejected rather than mis-decoded.
		for i := 0; i < len(b); i++ {
			if err := unmarshal(b[:i], reflect.New(reflect.TypeOf(m.out).Elem()).Interface()); err == nil {
				t.Fatalf("%T: truncated message of %d/%d bytes accepted", m.in, i, len(b))
			}
		}
	}
}

func FuzzUnmarshalAppendEntries(f *testing.F) {
	b, _ := marshal(nil, &raft.AppendEntriesRequest{Term: 1, LeaderID: "n1", Entries: []raft.Entry{{Index: 1, Term: 1, Data: []byte("x")}}})
	f.Add(b)
	f.Fuzz(func(t *testing.T, data []byte) {
		var m raft.AppendEntriesRequest
		_ = unmarshal(data, &m) // must never panic
	})
}

func BenchmarkMarshalAppendEntries(b *testing.B) {
	req := &raft.AppendEntriesRequest{Term: 3, LeaderID: "n1", PrevLogIndex: 10, PrevLogTerm: 2, LeaderCommit: 9}
	for i := 0; i < 32; i++ {
		req.Entries = append(req.Entries, raft.Entry{Index: uint64(11 + i), Term: 3, Data: make([]byte, 160)})
	}
	buf := make([]byte, 0, 8192)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, _ := marshal(buf[:0], req)
		var m raft.AppendEntriesRequest
		unmarshal(out, &m)
	}
}
