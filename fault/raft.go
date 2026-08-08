package fault

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AbeerDas/quorum/raft"
)

// Transport wraps the real Raft transport so a faulted node's outbound
// messages are held or dropped before they reach the network.
//
// Gating both directions is deliberate. Blocking only what a node receives
// would leave a "dead" node still voting and still heartbeating, which is not
// a failure any real machine produces.
type Transport struct {
	inner raft.Transport
	inj   *Injector
}

// NewTransport wraps a transport with an injector's current fault.
func NewTransport(inner raft.Transport, inj *Injector) *Transport {
	return &Transport{inner: inner, inj: inj}
}

// RequestVote implements raft.Transport.
func (t *Transport) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	if err := t.inj.Gate(ctx); err != nil {
		return nil, err
	}
	return t.inner.RequestVote(ctx, to, args)
}

// AppendEntries implements raft.Transport.
func (t *Transport) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	if err := t.inj.Gate(ctx); err != nil {
		return nil, err
	}
	return t.inner.AppendEntries(ctx, to, args)
}

// UnaryServerInterceptor gates every Raft RPC arriving at this node.
//
// The interceptor is the right seam for the receiving side: unlike the
// RPCHandler interface, it has both a context to respect and an error to
// return, so a paused node can hang until the caller's own deadline expires
// rather than leaking a goroutine per message.
func (i *Injector) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := i.Gate(ctx); err != nil {
			// Unavailable is the code a peer would see from a host that is not
			// there, so the caller's Raft logic treats it as an ordinary
			// unreachable peer instead of a protocol error.
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return handler(ctx, req)
	}
}
