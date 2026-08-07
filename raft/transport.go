package raft

import (
	"context"
	"time"
)

// NodeID identifies one member of the cluster.
type NodeID string

// LogEntry is one command in the replicated log.
//
// Command is opaque to Raft. Raft's only job is to make every node agree on the
// same commands in the same order; interpreting them is the state machine's job.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RequestVoteArgs is the RequestVote RPC request (Raft paper, Figure 2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request, used both to replicate log
// entries and, with no entries, as the leader's heartbeat.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response.
//
// ConflictIndex and ConflictTerm are not in Figure 2. They let a leader whose log
// has diverged skip straight past the conflicting run instead of walking back one
// entry per round trip, which the paper notes as a practical optimisation. They
// only affect how fast the logs converge, never what they converge to.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64

	// AppliedIndex is how far the follower has applied entries to its state
	// machine. The leader uses it to measure replication lag as the spec
	// defines it - commit on the leader through to apply on the follower -
	// rather than the easier but different "commit through to acknowledged".
	AppliedIndex uint64
}

// Transport sends Raft RPCs to other nodes. An error means the peer could not be
// reached, which Raft treats as an ordinary, expected condition rather than a
// failure: unreachable peers are exactly what the protocol exists to survive.
//
// Two implementations exist: gRPC for real deployments, and an in-memory network
// used by tests so links can be severed precisely and instantly.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPCHandler is the receiving side of Transport. A *Node implements it, and each
// transport delivers incoming RPCs here.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}

// StateMachine consumes committed commands in log order.
//
// Apply is called on every node with exactly the same commands in exactly the
// same order, which is what makes the replicas converge. It must therefore be
// deterministic: no clocks, no randomness, no map iteration order (PRD.md
// Section 14).
type StateMachine interface {
	Apply(entry LogEntry)
}

// Clock supplies time to the Raft node. Production uses the system clock; the
// indirection exists so timing can be virtualised later without touching the
// consensus logic.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
