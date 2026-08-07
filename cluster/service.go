package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// ErrProposalSuperseded means a leader change discarded the command before it
// committed.
//
// It is safe to retry. The entry at that position was replaced by a different
// one from a later term, which is proof the command never took effect - so
// retrying cannot count it twice. That proof is the whole reason this is a
// distinct error: a plain timeout carries no such guarantee.
var ErrProposalSuperseded = errors.New("cluster: proposal superseded by a leader change")

// raftNode is the slice of *raft.Node this package uses.
//
// It is an interface so the retry-safety rules can be driven directly in tests.
// Those rules only run when leadership changes in the middle of a single
// request, and a live cluster will not reproduce that on demand - which is
// exactly how they went untested until mutation testing exposed it.
type raftNode interface {
	Propose(command []byte) (index uint64, term uint64, err error)
	ID() raft.NodeID
	Role() raft.Role
	Term() uint64
	LeaderID() raft.NodeID
	CommitIndex() uint64
	PeerLastContact() map[raft.NodeID]time.Time
}

// Service turns rate-limit decisions into replicated Raft commands.
//
// Every decision is made by applying a committed log entry, never locally. That
// is slower than deciding on the spot - each request costs a round trip to a
// majority - but it is what makes the answer survive the node that gave it.
type Service struct {
	node raftNode
	fsm  *FSM

	// retryInterval spaces out retries after a superseded proposal, giving an
	// election time to finish.
	retryInterval time.Duration
	maxAttempts   int
}

// NewService wires a Raft node to its replicated limiter.
func NewService(node raftNode, fsm *FSM) *Service {
	return &Service{
		node:          node,
		fsm:           fsm,
		retryInterval: 10 * time.Millisecond,
		maxAttempts:   5,
	}
}

// Check decides whether one request from callerID may proceed, judged at the
// instant `at`.
//
// The instant is an argument rather than read here so the leader can stamp it
// into the log entry, letting every node apply the identical input.
//
// A node that is not the leader refuses immediately with a *raft.NotLeaderError
// naming who to ask. Forwarding is deliberately left to the caller: this layer
// answers "can I do this", and the API layer decides how to route.
func (s *Service) Check(ctx context.Context, callerID string, at time.Time) (limiter.Decision, error) {
	res, err := s.apply(ctx, ConsumeCommand(callerID, 1, at))
	if err != nil {
		return limiter.Decision{}, err
	}
	return res.Decision, nil
}

// CheckN spends an arbitrary number of tokens.
func (s *Service) CheckN(ctx context.Context, callerID string, amount float64, at time.Time) (limiter.Decision, error) {
	res, err := s.apply(ctx, ConsumeCommand(callerID, amount, at))
	if err != nil {
		return limiter.Decision{}, err
	}
	return res.Decision, nil
}

// SetLimit changes the limit across the whole cluster. The change is replicated
// like any other command, so nodes cannot end up enforcing different rules.
func (s *Service) SetLimit(ctx context.Context, limit int, window time.Duration, at time.Time) error {
	_, err := s.apply(ctx, ConfigCommand(limit, window, at))
	return err
}

// apply replicates one command and returns what applying it produced.
func (s *Service) apply(ctx context.Context, cmd Command) (Result, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxAttempts; attempt++ {
		res, err := s.applyOnce(ctx, cmd)
		if err == nil {
			return res, nil
		}
		lastErr = err

		// Only a superseded proposal is known not to have taken effect. Every
		// other failure - a timeout above all - leaves it genuinely unknown
		// whether the command committed, and retrying an unknown is how a
		// request gets counted twice.
		if !errors.Is(err, ErrProposalSuperseded) {
			return Result{}, err
		}

		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(s.retryInterval):
		}
	}

	return Result{}, lastErr
}

// applyOnce proposes a command and waits for the node to apply it.
func (s *Service) applyOnce(ctx context.Context, cmd Command) (Result, error) {
	encoded, err := cmd.Encode()
	if err != nil {
		return Result{}, err
	}

	index, term, err := s.node.Propose(encoded)
	if err != nil {
		// Includes *raft.NotLeaderError, which means the command was never
		// appended anywhere and is safe for the caller to send elsewhere.
		return Result{}, err
	}

	// Register the waiter only after proposing, since the index is not known
	// before then. The state machine retains recent results so an entry that
	// applies in the meantime is not missed.
	select {
	case res := <-s.fsm.Wait(index):
		if res.Term != term {
			// Something else occupies that position now, so this command was
			// discarded rather than executed.
			return Result{}, fmt.Errorf("%w: proposed at index %d in term %d, applied term %d",
				ErrProposalSuperseded, index, term, res.Term)
		}
		if res.Err != nil {
			return Result{}, res.Err
		}
		return res, nil

	case <-ctx.Done():
		// Deliberately not retried: the command may or may not have committed.
		return Result{}, ctx.Err()
	}
}

// Status describes this node's place in the cluster.
type Status struct {
	NodeID      raft.NodeID
	Role        string
	Term        uint64
	LeaderID    raft.NodeID
	CommitIndex uint64
}

// Status reports the node's current role and view of the cluster.
func (s *Service) Status() Status {
	return Status{
		NodeID:      s.node.ID(),
		Role:        s.node.Role().String(),
		Term:        s.node.Term(),
		LeaderID:    s.node.LeaderID(),
		CommitIndex: s.node.CommitIndex(),
	}
}

// PeerLastContact reports when each peer was last heard from, for /status.
func (s *Service) PeerLastContact() map[raft.NodeID]time.Time { return s.node.PeerLastContact() }

// Config is the limit currently in force on this node.
func (s *Service) Config() limiter.Config { return s.fsm.Config() }

// TrackedCallers is how many callers currently hold state.
func (s *Service) TrackedCallers() int { return s.fsm.TrackedCallers() }

// Snapshot copies the replicated state, for comparing nodes.
func (s *Service) Snapshot() map[string]limiter.BucketState { return s.fsm.Snapshot() }
