package api

import (
	"context"
	"time"

	"github.com/AbeerDas/quorum/cluster"
	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// Backend is the limiter the API serves.
//
// Two exist: a single node holding the limiter directly, and a cluster that
// replicates every decision through Raft. Keeping them behind one interface is
// what let Stage 2's single-node API stay working, and tested, while the
// replicated version was built alongside it.
type Backend interface {
	// Check decides one request, judged at instant `at`. The instant is passed
	// in rather than read inside so the cluster can stamp it into the log entry
	// and have every node apply the same input.
	Check(ctx context.Context, callerID string, at time.Time) (limiter.Decision, error)
	SetLimit(ctx context.Context, limit int, window time.Duration, at time.Time) error
	Config() limiter.Config
	TrackedCallers() int
	Status(at time.Time) ClusterStatus
}

// ClusterStatus is what a node reports about itself and its peers.
type ClusterStatus struct {
	NodeID   string
	Mode     string
	Role     string
	Term     uint64
	LeaderID string
	Peers    []peerStatus
}

// SingleNodeBackend runs the limiter directly, with no replication. It is what
// the node uses when no peers are configured.
type SingleNodeBackend struct {
	lim    *limiter.Limiter
	nodeID string
}

// NewSingleNodeBackend serves a limiter with no cluster behind it.
func NewSingleNodeBackend(l *limiter.Limiter, nodeID string) *SingleNodeBackend {
	return &SingleNodeBackend{lim: l, nodeID: nodeID}
}

func (b *SingleNodeBackend) Check(_ context.Context, callerID string, at time.Time) (limiter.Decision, error) {
	return b.lim.Allow(callerID, at), nil
}

func (b *SingleNodeBackend) SetLimit(_ context.Context, limit int, window time.Duration, _ time.Time) error {
	cfg := b.lim.Config()
	cfg.Limit = limit
	cfg.Window = window
	b.lim.SetConfig(cfg)
	return nil
}

func (b *SingleNodeBackend) Config() limiter.Config { return b.lim.Config() }
func (b *SingleNodeBackend) TrackedCallers() int    { return b.lim.Len() }

func (b *SingleNodeBackend) Status(time.Time) ClusterStatus {
	return ClusterStatus{
		NodeID: b.nodeID,
		Mode:   "single-node",
		// The only node in the system is by definition the one accepting
		// writes. Mode makes clear this was not won in an election.
		Role:     "leader",
		Term:     0,
		LeaderID: b.nodeID,
		Peers:    []peerStatus{},
	}
}

// ClusterBackend replicates every decision through Raft.
type ClusterBackend struct {
	svc   *cluster.Service
	peers map[raft.NodeID]string

	// healthyWithin is how recently a peer must have answered to count as
	// healthy. Anything beyond a few heartbeats means it has gone quiet.
	healthyWithin time.Duration
}

// NewClusterBackend serves a replicated limiter. peers maps each other node's
// ID to the address of its HTTP API, which is also where writes are forwarded.
func NewClusterBackend(svc *cluster.Service, peers map[raft.NodeID]string, healthyWithin time.Duration) *ClusterBackend {
	if healthyWithin <= 0 {
		healthyWithin = 2 * time.Second
	}
	copied := make(map[raft.NodeID]string, len(peers))
	for id, addr := range peers {
		copied[id] = addr
	}
	return &ClusterBackend{svc: svc, peers: copied, healthyWithin: healthyWithin}
}

func (b *ClusterBackend) Check(ctx context.Context, callerID string, at time.Time) (limiter.Decision, error) {
	return b.svc.Check(ctx, callerID, at)
}

func (b *ClusterBackend) SetLimit(ctx context.Context, limit int, window time.Duration, at time.Time) error {
	return b.svc.SetLimit(ctx, limit, window, at)
}

func (b *ClusterBackend) Config() limiter.Config { return b.svc.Config() }
func (b *ClusterBackend) TrackedCallers() int    { return b.svc.TrackedCallers() }

// PeerAddr is where a given node's HTTP API lives, used to forward writes.
func (b *ClusterBackend) PeerAddr(id raft.NodeID) (string, bool) {
	addr, ok := b.peers[id]
	return addr, ok
}

func (b *ClusterBackend) Status(at time.Time) ClusterStatus {
	st := b.svc.Status()
	contact := b.svc.PeerLastContact()

	peers := make([]peerStatus, 0, len(b.peers))
	for id, addr := range b.peers {
		p := peerStatus{
			NodeID:  string(id),
			Address: addr,
			// Only a leader contacts peers, so a follower has nothing to report.
			// -1 says "not known from here" rather than falsely claiming the
			// peer was last seen at the epoch.
			LastSeenMS: -1,
		}
		if seen, ok := contact[id]; ok && !seen.IsZero() {
			since := at.Sub(seen)
			p.LastSeenMS = since.Milliseconds()
			p.Healthy = since <= b.healthyWithin
		}
		peers = append(peers, p)
	}

	return ClusterStatus{
		NodeID:   string(st.NodeID),
		Mode:     "cluster",
		Role:     st.Role,
		Term:     st.Term,
		LeaderID: string(st.LeaderID),
		Peers:    peers,
	}
}
