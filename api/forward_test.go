package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// stubBackend stands in for a node that is not the leader, so the routing
// behaviour can be driven without standing up a real cluster.
type stubBackend struct {
	leader   raft.NodeID
	addrs    map[raft.NodeID]string
	attempts atomic.Int64

	// err is what Check and SetLimit return. Nil means the request succeeds
	// locally, as it would once this node became the leader. Guarded because a
	// test flips it from another goroutine to simulate an election completing
	// mid-request.
	mu  sync.Mutex
	err error
}

func (b *stubBackend) setErr(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

func (b *stubBackend) currentErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (b *stubBackend) Check(context.Context, string, time.Time) (limiter.Decision, error) {
	b.attempts.Add(1)
	if err := b.currentErr(); err != nil {
		return limiter.Decision{}, err
	}
	return limiter.Decision{Allowed: true, Remaining: 7}, nil
}

func (b *stubBackend) SetLimit(context.Context, int, time.Duration, time.Time) error {
	b.attempts.Add(1)
	return b.currentErr()
}

func (b *stubBackend) Config() limiter.Config {
	return limiter.Config{Limit: 10, Window: time.Minute}
}

func (b *stubBackend) TrackedCallers() int { return 0 }

func (b *stubBackend) Status(time.Time) ClusterStatus {
	return ClusterStatus{NodeID: "node-follower", Mode: "cluster", Role: "follower", Term: 4,
		LeaderID: string(b.leader), Peers: []peerStatus{}}
}

func (b *stubBackend) PeerAddr(id raft.NodeID) (string, bool) {
	addr, ok := b.addrs[id]
	return addr, ok
}

func newForwardingServer(t *testing.T, backend Backend, grace time.Duration) *Server {
	t.Helper()
	return NewServer(ServerConfig{
		Backend:       backend,
		NodeID:        "node-follower",
		Now:           func() time.Time { return t0 },
		FailoverGrace: grace,
	})
}

// A request landing on a follower must be answered, not refused: the caller
// should never need to know which node holds leadership.
func TestFollowerForwardsCheckToLeaderAndReturnsItsAnswer(t *testing.T) {
	var gotForwardHeader string
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotForwardHeader = r.Header.Get(forwardedHeader)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"allowed":true,"remaining":3}`))
	}))
	defer leader.Close()

	backend := &stubBackend{
		leader: "node-leader",
		addrs:  map[raft.NodeID]string{"node-leader": leader.Listener.Addr().String()},
		err:    &raft.NotLeaderError{Leader: "node-leader"},
	}
	s := newForwardingServer(t, backend, 0)

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := decode[checkResponse](t, rec); !got.Allowed || got.Remaining != 3 {
		t.Errorf("response = %+v, want the leader's answer {allowed:true remaining:3}", got)
	}
	if gotForwardHeader == "" {
		t.Error("the forwarded request carried no marker, so a loop could not be detected")
	}
}

// A blocked verdict from the leader must reach the caller intact, status code
// and retry hint included.
func TestForwardedBlockedResponseIsRelayedFaithfully(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "12")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"allowed":false,"remaining":0,"retry_after_ms":11500}`))
	}))
	defer leader.Close()

	backend := &stubBackend{
		leader: "node-leader",
		addrs:  map[raft.NodeID]string{"node-leader": leader.Listener.Addr().String()},
		err:    &raft.NotLeaderError{Leader: "node-leader"},
	}
	s := newForwardingServer(t, backend, 0)

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Retry-After"); got != "12" {
		t.Errorf("Retry-After = %q, want %q", got, "12")
	}
	if got := decode[checkResponse](t, rec); got.RetryAfterMS != 11500 {
		t.Errorf("retry_after_ms = %d, want 11500", got.RetryAfterMS)
	}
}

// A request that has already been forwarded once must not be forwarded again.
// Leadership moving mid-flight should surface as an honest error rather than a
// request circling the cluster.
func TestAlreadyForwardedRequestIsNotForwardedAgain(t *testing.T) {
	forwarded := atomic.Int64{}
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer leader.Close()

	backend := &stubBackend{
		leader: "node-leader",
		addrs:  map[raft.NodeID]string{"node-leader": leader.Listener.Addr().String()},
		err:    &raft.NotLeaderError{Leader: "node-leader"},
	}
	s := newForwardingServer(t, backend, 0)

	req := httptest.NewRequest(http.MethodPost, "/check", stringReader(`{"caller_id":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedHeader, "1")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if n := forwarded.Load(); n != 0 {
		t.Errorf("forwarded %d times, want 0: an already-forwarded request was passed on again", n)
	}
}

// When the leader cannot be reached at all, the request was never delivered, so
// it is safe to try again once a new leader appears. This is what makes a
// failover look seamless rather than producing a burst of errors.
func TestUnreachableLeaderIsRetriedUntilOneAppears(t *testing.T) {
	backend := &stubBackend{
		leader: "node-leader",
		// Port 1 on loopback refuses connections immediately, so the request
		// provably never arrives anywhere.
		addrs: map[raft.NodeID]string{"node-leader": "127.0.0.1:1"},
		err:   &raft.NotLeaderError{Leader: "node-leader"},
	}
	s := newForwardingServer(t, backend, 200*time.Millisecond)

	// Partway through the grace period this node becomes the leader itself, as
	// it would after winning an election.
	go func() {
		time.Sleep(40 * time.Millisecond)
		backend.setErr(nil)
	}()

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: the request should have survived the election (body %s)",
			rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := decode[checkResponse](t, rec); !got.Allowed {
		t.Error("allowed = false, want true once a leader was available")
	}
	if n := backend.attempts.Load(); n < 2 {
		t.Errorf("backend consulted %d time(s), want more than 1: it should have retried", n)
	}
}

// With no grace period configured, an unreachable leader fails immediately
// rather than hanging.
func TestUnreachableLeaderFailsWhenNoGraceIsConfigured(t *testing.T) {
	backend := &stubBackend{
		leader: "node-leader",
		addrs:  map[raft.NodeID]string{"node-leader": "127.0.0.1:1"},
		err:    &raft.NotLeaderError{Leader: "node-leader"},
	}
	s := newForwardingServer(t, backend, 0)

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// During an election nobody is leader, so there is no address to forward to.
// The node must say so plainly instead of guessing.
func TestNoKnownLeaderIsReportedNotGuessed(t *testing.T) {
	backend := &stubBackend{
		leader: "",
		addrs:  map[raft.NodeID]string{},
		err:    &raft.NotLeaderError{Leader: ""},
	}
	s := newForwardingServer(t, backend, 0)

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}
