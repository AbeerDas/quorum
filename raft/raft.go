package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Role is a node's current position in the protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// NotLeaderError is returned when a write is offered to a node that is not the
// leader. Leader names the node believed to be in charge, so a caller can be
// redirected instead of simply failing. It is empty during an election.
type NotLeaderError struct {
	Leader NodeID
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not the leader (no leader currently known)"
	}
	return fmt.Sprintf("raft: not the leader (try %s)", e.Leader)
}

// Config describes one node's participation in a cluster.
type Config struct {
	ID    NodeID
	Peers []NodeID // the other members, not including ID

	Transport    Transport
	StateMachine StateMachine

	// ElectionTimeoutMin/Max bound the randomised wait before a follower gives
	// up on the leader. The range must be wide enough that nodes rarely time out
	// together, since simultaneous candidates split the vote and force a retry.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval must be comfortably shorter than ElectionTimeoutMin, or
	// followers will start elections against a leader that is perfectly healthy.
	HeartbeatInterval time.Duration

	Clock  Clock
	Logger *slog.Logger
}

// Node is one member of a Raft cluster.
//
// State is guarded by a single mutex. The rule that keeps it deadlock-free: the
// lock is never held across an outbound RPC. Every replication path snapshots
// what it needs, releases the lock, makes the call, then re-acquires and
// re-checks that the world has not moved on underneath it.
type Node struct {
	id           NodeID
	peers        []NodeID
	transport    Transport
	sm           StateMachine
	clock        Clock
	logger       *slog.Logger
	electionMin  time.Duration
	electionMax  time.Duration
	heartbeat    time.Duration
	rpcTimeout   time.Duration

	mu          sync.Mutex
	role        Role
	currentTerm uint64
	votedFor    NodeID
	leaderID    NodeID

	// log is 1-indexed with a sentinel at position 0, so a slice position is
	// also its Raft index. Valid only because log compaction is out of scope.
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	votes      int

	wake     chan struct{}
	applyCh  chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewNode creates a node. It does not participate until Start is called.
func NewNode(cfg Config) *Node {
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.ElectionTimeoutMin / 4
	}

	return &Node{
		id:          cfg.ID,
		peers:       append([]NodeID(nil), cfg.Peers...),
		transport:   cfg.Transport,
		sm:          cfg.StateMachine,
		clock:       cfg.Clock,
		logger:      cfg.Logger.With("node_id", string(cfg.ID)),
		electionMin: cfg.ElectionTimeoutMin,
		electionMax: cfg.ElectionTimeoutMax,
		heartbeat:   cfg.HeartbeatInterval,
		rpcTimeout:  cfg.ElectionTimeoutMax,

		role: Follower,
		// The sentinel lets index 0 mean "before the log began", so PrevLogIndex
		// needs no special case for an empty log.
		log:        []LogEntry{{Term: 0, Index: 0}},
		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),

		wake:    make(chan struct{}, 1),
		applyCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

// Start begins participating in the cluster.
func (n *Node) Start() {
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
}

// Stop halts the node permanently. It does not come back: without durable
// storage a restarted node would have forgotten which term it voted in, and
// could vote twice in the same term, allowing two leaders at once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.done)
	})
	n.wg.Wait()
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// LeaderID is the node currently believed to lead, or empty during an election.
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// CommitIndex is the highest log position known to be safely replicated.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// Propose submits a command for replication. Only the leader may accept one:
// allowing a follower to write would create a second source of truth, which is
// precisely what consensus exists to prevent.
func (n *Node) Propose(command []byte) (index uint64, term uint64, err error) {
	n.mu.Lock()

	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		return 0, 0, &NotLeaderError{Leader: leader}
	}

	index = uint64(len(n.log))
	term = n.currentTerm
	n.log = append(n.log, LogEntry{Term: term, Index: index, Command: append([]byte(nil), command...)})
	n.matchIndex[n.id] = index
	n.mu.Unlock()

	// Push it out immediately rather than waiting for the next heartbeat.
	n.signalWake()
	return index, term, nil
}

// run is the main loop: it drives elections while a follower or candidate, and
// sends heartbeats while leader.
func (n *Node) run() {
	defer n.wg.Done()

	for {
		select {
		case <-n.done:
			return
		default:
		}

		if n.Role() == Leader {
			n.leaderTick()
		} else {
			n.followerTick()
		}
	}
}

// followerTick waits for a heartbeat; if none arrives in time, it starts an
// election.
func (n *Node) followerTick() {
	select {
	case <-n.done:
	case <-n.wake:
		// Heard from a leader or granted a vote: the timer restarts.
	case <-n.clock.After(n.randomElectionTimeout()):
		n.startElection()
	}
}

func (n *Node) leaderTick() {
	n.broadcastAppendEntries()

	select {
	case <-n.done:
	case <-n.wake:
	case <-n.clock.After(n.heartbeat):
	}
}

// randomElectionTimeout spreads nodes out so they rarely become candidates at
// the same moment. Without the randomisation, a cluster can livelock: every node
// times out together, every node votes for itself, nobody wins, repeat.
func (n *Node) randomElectionTimeout() time.Duration {
	spread := n.electionMax - n.electionMin
	return n.electionMin + time.Duration(rand.Int63n(int64(spread)+1))
}

func (n *Node) majority() int {
	return (len(n.peers)+1)/2 + 1
}

func (n *Node) signalWake() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}

	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = ""
	n.votes = 1 // its own

	term := n.currentTerm
	lastIndex, lastTerm := n.lastLogLocked()
	peers := append([]NodeID(nil), n.peers...)
	n.mu.Unlock()

	n.logger.Debug("election started", "term", term)

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	for _, peer := range peers {
		go n.solicitVote(peer, term, args)
	}
}

func (n *Node) solicitVote(peer NodeID, term uint64, args *RequestVoteArgs) {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()

	reply, err := n.transport.RequestVote(ctx, peer, args)
	if err != nil {
		// An unreachable peer is normal; it simply does not vote.
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	// Discard a reply that arrived after the world moved on.
	if n.role != Candidate || n.currentTerm != term {
		return
	}
	if !reply.VoteGranted {
		return
	}

	n.votes++
	if n.votes >= n.majority() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = ""
	n.signalWake()
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id

	lastIndex, _ := n.lastLogLocked()
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = lastIndex

	n.logger.Info("became leader", "term", n.currentTerm, "last_log_index", lastIndex)
	n.signalWake()
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := append([]NodeID(nil), n.peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateTo(peer)
	}
}

// replicateTo brings one follower up to date, or heartbeats it if it already is.
func (n *Node) replicateTo(peer NodeID) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1
	if prevIndex >= uint64(len(n.log)) {
		prevIndex = uint64(len(n.log)) - 1
		next = prevIndex + 1
	}
	prevTerm := n.log[prevIndex].Term
	entries := append([]LogEntry(nil), n.log[next:]...)

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()

	reply, err := n.transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		match := prevIndex + uint64(len(entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
		return
	}

	// The follower's log diverges. Back up and try again from further back.
	n.nextIndex[peer] = n.backoffLocked(reply)
}

// backoffLocked picks where to resume replication after a rejected AppendEntries,
// using the follower's conflict hint to skip the whole conflicting run at once.
func (n *Node) backoffLocked(reply *AppendEntriesReply) uint64 {
	if reply.ConflictTerm != 0 {
		for i := len(n.log) - 1; i > 0; i-- {
			if n.log[i].Term == reply.ConflictTerm {
				return uint64(i) + 1
			}
		}
	}
	if reply.ConflictIndex < 1 {
		return 1
	}
	return reply.ConflictIndex
}

// advanceCommitLocked moves the commit point forward once a majority has stored
// an entry.
func (n *Node) advanceCommitLocked() {
	lastIndex, _ := n.lastLogLocked()

	for index := lastIndex; index > n.commitIndex; index-- {
		// Figure 2 forbids committing an entry from an earlier term on replica
		// count alone. An entry can be on a majority of nodes and still be
		// overwritten later; only committing a current-term entry makes it, and
		// everything before it, permanent.
		if n.log[index].Term != n.currentTerm {
			continue
		}

		stored := 0
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				stored++
			}
		}
		stored++ // the leader itself

		if stored >= n.majority() {
			n.commitIndex = index
			n.signalApply()
			return
		}
	}
}

func (n *Node) lastLogLocked() (index, term uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// HandleRequestVote answers a candidate asking for support.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	reply := &RequestVoteReply{Term: n.currentTerm}

	if args.Term < n.currentTerm {
		return reply
	}
	if n.votedFor != "" && n.votedFor != args.CandidateID {
		return reply
	}
	// A candidate missing committed entries must never win, or those entries
	// would be lost. This check is what makes the commitment durable.
	if !n.candidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		return reply
	}

	n.votedFor = args.CandidateID
	reply.VoteGranted = true
	n.signalWake() // granting a vote restarts the election timer

	return reply
}

// candidateUpToDateLocked implements Raft's "at least as up-to-date" comparison:
// a later last term wins; on equal terms, the longer log wins.
func (n *Node) candidateUpToDateLocked(lastIndex, lastTerm uint64) bool {
	myIndex, myTerm := n.lastLogLocked()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// HandleAppendEntries receives replicated entries, or a bare heartbeat.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	reply := &AppendEntriesReply{Term: n.currentTerm}

	// A stale leader must be rejected so it learns it has been superseded.
	if args.Term < n.currentTerm {
		return reply
	}

	// Same term as a live leader: stand down and follow it.
	n.role = Follower
	n.leaderID = args.LeaderID
	n.signalWake()

	lastIndex, _ := n.lastLogLocked()

	// The log is shorter than the leader assumes.
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return reply
	}

	// The entry at PrevLogIndex disagrees, so everything from that term is
	// suspect. Report the whole run so the leader can skip it in one round trip.
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := n.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > 0 && n.log[index].Term == conflictTerm {
			index--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = index + 1
		return reply
	}

	// The logs agree up to PrevLogIndex. Append what is new, truncating anything
	// that contradicts the leader.
	for i := range args.Entries {
		index := args.PrevLogIndex + 1 + uint64(i)
		if index < uint64(len(n.log)) {
			if n.log[index].Term == args.Entries[i].Term {
				continue
			}
			n.log = n.log[:index]
		}
		n.log = append(n.log, args.Entries[i:]...)
		break
	}

	if args.LeaderCommit > n.commitIndex {
		lastIndex, _ = n.lastLogLocked()
		n.commitIndex = min(args.LeaderCommit, lastIndex)
		n.signalApply()
	}

	reply.Success = true
	return reply
}

// applyLoop hands committed entries to the state machine, in order. Running it
// outside the lock keeps a slow state machine from stalling consensus.
func (n *Node) applyLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.done:
			return
		case <-n.applyCh:
		}

		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			n.lastApplied++
			entry := n.log[n.lastApplied]
			n.mu.Unlock()

			if n.sm != nil {
				n.sm.Apply(entry)
			}
		}
	}
}

// ErrStopped is returned by operations on a node that has been shut down.
var ErrStopped = errors.New("raft: node stopped")
