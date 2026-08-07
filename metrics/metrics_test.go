package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/raft"
)

// fakeClock lets the window tests drive time instead of sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func scrape(t *testing.T, c *Collector) string {
	t.Helper()
	rec := httptest.NewRecorder()
	c.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("scrape status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func TestMetricsAreExposedInPrometheusFormat(t *testing.T) {
	c := New(Options{NodeID: "node-1"})

	c.ObserveRequest(Allowed, 2*time.Millisecond)
	c.ObserveRequest(Blocked, 3*time.Millisecond)
	c.ElectionSettled(4, 120*time.Millisecond, true)
	c.ReplicationLag("node-2", 5*time.Millisecond)
	c.CommitIndexAdvanced(42)
	c.RoleChanged(raft.Leader, 4)
	c.SetPeerHealthy("node-2", true)

	body := scrape(t, c)

	// Every metric PRD.md Section 10 requires must actually appear.
	required := []string{
		"quorum_raft_election_duration_seconds",
		"quorum_raft_replication_lag_seconds",
		"quorum_request_duration_seconds",
		"quorum_rejected_requests_total",
		"quorum_raft_role",
		"quorum_raft_commit_index",
		"quorum_peer_healthy",
	}
	for _, name := range required {
		if !strings.Contains(body, name) {
			t.Errorf("scrape is missing %q", name)
		}
	}

	// Values must be real, not zero placeholders.
	if !strings.Contains(body, `quorum_raft_commit_index{node_id="node-1"} 42`) {
		t.Error("commit index was not reported as 42")
	}
	if !strings.Contains(body, `quorum_raft_role{node_id="node-1",role="leader"} 1`) {
		t.Error("role gauge does not show this node as leader")
	}
}

// Only one role may be current at a time, or a dashboard reading the gauge
// would show a node as two things at once.
func TestOnlyTheCurrentRoleReadsAsOne(t *testing.T) {
	c := New(Options{NodeID: "node-1"})

	c.RoleChanged(raft.Candidate, 2)
	body := scrape(t, c)

	if !strings.Contains(body, `role="candidate"} 1`) {
		t.Error("candidate role is not set to 1")
	}
	for _, stale := range []string{`role="leader"} 1`, `role="follower"} 1`} {
		if strings.Contains(body, stale) {
			t.Errorf("a previous role is still reported as current: %s", stale)
		}
	}
}

// A caller exceeding its budget and a request arriving at the wrong node are
// different events. Counting them together would make routing churn during a
// failover look like abusive traffic.
func TestRejectionsAreSeparatedByReason(t *testing.T) {
	c := New(Options{NodeID: "node-1"})

	c.ObserveRequest(Blocked, time.Millisecond) // over limit
	c.ObserveRejection(ReasonWrongNode)
	c.ObserveRejection(ReasonWrongNode)

	body := scrape(t, c)

	if !strings.Contains(body, `quorum_rejected_requests_total{node_id="node-1",reason="over_limit"} 1`) {
		t.Error("over-limit rejections were not counted separately")
	}
	if !strings.Contains(body, `quorum_rejected_requests_total{node_id="node-1",reason="wrong_node"} 2`) {
		t.Error("wrong-node rejections were not counted separately")
	}
}

func TestRecentLatencyReportsPercentiles(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c := New(Options{NodeID: "node-1", RecentWindow: time.Minute, Now: clock.Now})

	for i := 1; i <= 100; i++ {
		c.ObserveRequest(Allowed, time.Duration(i)*time.Millisecond)
	}

	got := c.RecentLatency()
	if got.Samples != 100 {
		t.Fatalf("samples = %d, want 100", got.Samples)
	}
	if got.P50 != 51*time.Millisecond {
		t.Errorf("p50 = %v, want 51ms", got.P50)
	}
	if got.P99 != 100*time.Millisecond {
		t.Errorf("p99 = %v, want 100ms", got.P99)
	}
	if got.P50 >= got.P95 || got.P95 > got.P99 {
		t.Errorf("percentiles are not ordered: p50=%v p95=%v p99=%v", got.P50, got.P95, got.P99)
	}
}

// The whole point of a rolling window: a spike must age out, or the failover
// demo would show a permanently elevated number instead of a recovery.
func TestOldSamplesLeaveTheWindow(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c := New(Options{NodeID: "node-1", RecentWindow: 10 * time.Second, Now: clock.Now})

	// A burst of slow requests, as during a failover.
	for i := 0; i < 50; i++ {
		c.ObserveRequest(Allowed, 500*time.Millisecond)
	}
	if got := c.RecentLatency().P50; got != 500*time.Millisecond {
		t.Fatalf("p50 during the spike = %v, want 500ms", got)
	}

	// Time passes and normal traffic resumes.
	clock.advance(11 * time.Second)
	for i := 0; i < 50; i++ {
		c.ObserveRequest(Allowed, time.Millisecond)
	}

	got := c.RecentLatency()
	if got.Samples != 50 {
		t.Errorf("samples = %d, want 50: the spike did not age out", got.Samples)
	}
	if got.P50 != time.Millisecond {
		t.Errorf("p50 after recovery = %v, want 1ms: the old spike is still counted", got.P50)
	}
}

func TestEmptyWindowReportsNothingRatherThanZeroLatency(t *testing.T) {
	c := New(Options{NodeID: "node-1"})

	got := c.RecentLatency()
	if got.Samples != 0 {
		t.Errorf("samples = %d, want 0", got.Samples)
	}
}

func TestCollectorIsSafeUnderConcurrentUse(t *testing.T) {
	c := New(Options{NodeID: "node-1"})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.ObserveRequest(Allowed, time.Duration(i)*time.Microsecond)
			c.ReplicationLag("node-2", time.Millisecond)
			c.RoleChanged(raft.Follower, uint64(i))
			c.ObserveRejection(ReasonWrongNode)
			_ = c.RecentLatency()
		}(i)
	}
	wg.Wait()

	if got := c.RecentLatency().Samples; got != 50 {
		t.Errorf("samples = %d, want 50", got)
	}
}
