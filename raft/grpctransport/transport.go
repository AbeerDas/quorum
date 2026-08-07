// Package grpctransport carries Raft RPCs between nodes over gRPC.
//
// It is deliberately a separate package from raft: the consensus core stays
// free of any network dependency, which is what lets the correctness tests run
// against a simulated network where links can be severed precisely.
package grpctransport

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AbeerDas/quorum/raft"
	"github.com/AbeerDas/quorum/raft/raftpb"
)

// Transport dials peers over gRPC. Connections are created on first use and
// reused, since gRPC handles reconnection internally - a peer that is down
// simply produces errors until it returns, which is exactly what Raft expects.
type Transport struct {
	mu       sync.Mutex
	addrs    map[raft.NodeID]string
	conns    map[raft.NodeID]*grpc.ClientConn
	dialOpts []grpc.DialOption
	closed   bool
}

// New creates a transport that reaches each peer at the given address.
//
// Connections are insecure by default: transport security is an explicit
// non-goal for this build (PRD.md Section 16), and the cluster is expected to
// run on a private network. Pass dial options to change that.
func New(addrs map[raft.NodeID]string, dialOpts ...grpc.DialOption) *Transport {
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}

	copied := make(map[raft.NodeID]string, len(addrs))
	for id, addr := range addrs {
		copied[id] = addr
	}

	return &Transport{
		addrs:    copied,
		conns:    make(map[raft.NodeID]*grpc.ClientConn),
		dialOpts: dialOpts,
	}
}

// client returns a cached connection to the peer, dialing if needed.
func (t *Transport) client(to raft.NodeID) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, fmt.Errorf("grpctransport: transport closed")
	}
	if conn, ok := t.conns[to]; ok {
		return raftpb.NewRaftClient(conn), nil
	}

	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("grpctransport: no address configured for %s", to)
	}

	// NewClient does not block on connecting, so a peer that is currently down
	// does not stall the caller. The RPC itself surfaces the failure.
	conn, err := grpc.NewClient(addr, t.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: dial %s at %s: %w", to, addr, err)
	}

	t.conns[to] = conn
	return raftpb.NewRaftClient(conn), nil
}

// RequestVote implements raft.Transport.
func (t *Transport) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}

	resp, err := c.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  string(args.CandidateID),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}

	return &raft.RequestVoteReply{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}

// AppendEntries implements raft.Transport.
func (t *Transport) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	c, err := t.client(to)
	if err != nil {
		return nil, err
	}

	resp, err := c.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entriesToProto(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &raft.AppendEntriesReply{
		Term:          resp.GetTerm(),
		Success:       resp.GetSuccess(),
		ConflictIndex: resp.GetConflictIndex(),
		ConflictTerm:  resp.GetConflictTerm(),
		AppliedIndex:  resp.GetAppliedIndex(),
	}, nil
}

// Close shuts every connection down.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	var firstErr error
	for _, conn := range t.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)
	return firstErr
}

// Server exposes a node's RPC handlers as a gRPC service.
type Server struct {
	raftpb.UnimplementedRaftServer
	handler raft.RPCHandler
}

// Register wires a node into a gRPC server.
func Register(s grpc.ServiceRegistrar, handler raft.RPCHandler) {
	raftpb.RegisterRaftServer(s, &Server{handler: handler})
}

// RequestVote delivers an incoming vote request to the node.
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := s.handler.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.GetTerm(),
		CandidateID:  raft.NodeID(req.GetCandidateId()),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})

	return &raftpb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

// AppendEntries delivers incoming replication traffic to the node.
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	reply := s.handler.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.GetTerm(),
		LeaderID:     raft.NodeID(req.GetLeaderId()),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      entriesFromProto(req.GetEntries()),
		LeaderCommit: req.GetLeaderCommit(),
	})

	return &raftpb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
		AppliedIndex:  reply.AppliedIndex,
	}, nil
}

func entriesToProto(entries []raft.LogEntry) []*raftpb.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &raftpb.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		})
	}
	return out
}

func entriesFromProto(entries []*raftpb.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, raft.LogEntry{
			Term:    e.GetTerm(),
			Index:   e.GetIndex(),
			Command: e.GetCommand(),
		})
	}
	return out
}
