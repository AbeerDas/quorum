package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/limiter"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeClock lets tests drive time explicitly instead of sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestServer(t *testing.T, cfg limiter.Config) (*Server, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: t0}
	srv := NewServer(ServerConfig{
		Backend: NewSingleNodeBackend(limiter.New(cfg), "node-test"),
		NodeID:  "node-test",
		Now:     clock.Now,
	})
	return srv, clock
}

// do issues a request against the server and returns the recorder.
func do(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else if raw, ok := body.(string); ok {
		reader = bytes.NewReader([]byte(raw))
	} else {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

func stringReader(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestCheckAllowsUpToLimitThenBlocks(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 3, Window: time.Minute})

	for i := 1; i <= 3; i++ {
		rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i, rec.Code, http.StatusOK)
		}
		if got := decode[checkResponse](t, rec); !got.Allowed {
			t.Fatalf("request %d: allowed = false, want true", i)
		}
	}

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("over-limit request: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := decode[checkResponse](t, rec); got.Allowed {
		t.Error("over-limit request: allowed = true, want false")
	}
}

func TestCheckReportsRemaining(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 3, Window: time.Minute})

	for i, want := range []int{2, 1, 0} {
		rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
		if got := decode[checkResponse](t, rec).Remaining; got != want {
			t.Errorf("request %d: remaining = %d, want %d", i+1, got, want)
		}
	}
}

func TestBlockedResponseTellsClientWhenToRetry(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 1, Window: time.Minute})
	do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	got := decode[checkResponse](t, rec)

	if got.RetryAfterMS != time.Minute.Milliseconds() {
		t.Errorf("retry_after_ms = %d, want %d", got.RetryAfterMS, time.Minute.Milliseconds())
	}
	// Standard HTTP clients look for this header, in whole seconds.
	if h := rec.Header().Get("Retry-After"); h != "60" {
		t.Errorf("Retry-After header = %q, want %q", h, "60")
	}
}

func TestAllowedResponseOmitsRetryAfter(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 2, Window: time.Minute})

	rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	if bytes.Contains(rec.Body.Bytes(), []byte("retry_after_ms")) {
		t.Errorf("allowed response should not carry retry_after_ms, got %s", rec.Body.String())
	}
}

func TestCheckRefillsOverTime(t *testing.T) {
	s, clock := newTestServer(t, limiter.Config{Limit: 2, Window: time.Minute})
	do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})

	if rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"}); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d before refill", rec.Code, http.StatusTooManyRequests)
	}

	clock.Advance(30 * time.Second) // half a window restores one token

	if rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"}); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d after refill", rec.Code, http.StatusOK)
	}
}

// The fairness promise, end to end over HTTP.
func TestCallersAreIsolatedOverHTTP(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 1, Window: time.Minute})

	do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "noisy"})
	if rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "noisy"}); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("noisy caller: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	if rec := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "quiet"}); rec.Code != http.StatusOK {
		t.Errorf("quiet caller: status = %d, want %d (must be unaffected by noisy)", rec.Code, http.StatusOK)
	}
}

func TestCheckRejectsBadRequests(t *testing.T) {
	tests := []struct {
		name string
		body any
		want int
	}{
		{"missing caller_id", map[string]string{}, http.StatusBadRequest},
		{"empty caller_id", map[string]string{"caller_id": ""}, http.StatusBadRequest},
		{"malformed json", `{"caller_id":`, http.StatusBadRequest},
		{"wrong type", `{"caller_id": 42}`, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestServer(t, limiter.Config{Limit: 5, Window: time.Minute})
			if rec := do(t, s, http.MethodPost, "/check", tc.body); rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestEndpointsRejectWrongMethod(t *testing.T) {
	tests := []struct{ method, path string }{
		{http.MethodGet, "/check"},
		{http.MethodPost, "/status"},
		{http.MethodPost, "/config"},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			s, _ := newTestServer(t, limiter.Config{Limit: 5, Window: time.Minute})
			if rec := do(t, s, tc.method, tc.path, nil); rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

// Until Raft exists there is no cluster, and /status must say so plainly rather
// than imply an election has happened.
func TestStatusReportsHonestSingleNodeState(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 7, Window: 30 * time.Second})

	rec := do(t, s, http.MethodGet, "/status", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	got := decode[statusResponse](t, rec)

	if got.Mode != "single-node" {
		t.Errorf("mode = %q, want %q", got.Mode, "single-node")
	}
	if got.Term != 0 {
		t.Errorf("term = %d, want 0 (no election has occurred)", got.Term)
	}
	if len(got.Peers) != 0 {
		t.Errorf("peers = %v, want none", got.Peers)
	}
	if got.NodeID != "node-test" {
		t.Errorf("node_id = %q, want %q", got.NodeID, "node-test")
	}
	if got.Limit != 7 || got.WindowMS != 30_000 {
		t.Errorf("limit/window = %d/%d, want 7/30000", got.Limit, got.WindowMS)
	}
}

func TestStatusCountsAllowedAndBlocked(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 2, Window: time.Minute})

	for i := 0; i < 5; i++ { // 2 allowed, 3 blocked
		do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "alice"})
	}
	// Rejected malformed requests must not be counted as rate-limit decisions.
	do(t, s, http.MethodPost, "/check", `{"bad":`)

	got := decode[statusResponse](t, do(t, s, http.MethodGet, "/status", nil))
	if got.AllowedTotal != 2 {
		t.Errorf("allowed_total = %d, want 2", got.AllowedTotal)
	}
	if got.BlockedTotal != 3 {
		t.Errorf("blocked_total = %d, want 3", got.BlockedTotal)
	}
	if got.TrackedCallers != 1 {
		t.Errorf("tracked_callers = %d, want 1", got.TrackedCallers)
	}
}

func TestConfigUpdatesLimitLive(t *testing.T) {
	s, _ := newTestServer(t, limiter.Config{Limit: 1, Window: time.Minute})

	rec := do(t, s, http.MethodPut, "/config", map[string]any{"limit": 5, "window_ms": 10_000})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusOK, rec.Body.String())
	}

	// A caller seen after the change gets the new allowance.
	for i := 1; i <= 5; i++ {
		if r := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "bob"}); r.Code != http.StatusOK {
			t.Fatalf("bob request %d: status = %d, want %d", i, r.Code, http.StatusOK)
		}
	}
	if r := do(t, s, http.MethodPost, "/check", map[string]string{"caller_id": "bob"}); r.Code != http.StatusTooManyRequests {
		t.Error("bob exceeded the new limit without being blocked")
	}

	got := decode[statusResponse](t, do(t, s, http.MethodGet, "/status", nil))
	if got.Limit != 5 || got.WindowMS != 10_000 {
		t.Errorf("status limit/window = %d/%d, want 5/10000", got.Limit, got.WindowMS)
	}
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		body any
	}{
		{"zero limit", map[string]any{"limit": 0, "window_ms": 1000}},
		{"negative limit", map[string]any{"limit": -1, "window_ms": 1000}},
		{"zero window", map[string]any{"limit": 5, "window_ms": 0}},
		{"negative window", map[string]any{"limit": 5, "window_ms": -1000}},
		{"malformed json", `{"limit":`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestServer(t, limiter.Config{Limit: 3, Window: time.Minute})
			if rec := do(t, s, http.MethodPut, "/config", tc.body); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			// A rejected change must leave the limit untouched.
			got := decode[statusResponse](t, do(t, s, http.MethodGet, "/status", nil))
			if got.Limit != 3 {
				t.Errorf("limit = %d after rejected change, want 3", got.Limit)
			}
		})
	}
}

func TestConcurrentChecksAreCountedExactly(t *testing.T) {
	const limit = 50
	s, _ := newTestServer(t, limiter.Config{Limit: limit, Window: time.Hour})

	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			do2(s, map[string]string{"caller_id": "alice"})
		}()
	}
	wg.Wait()

	got := decode[statusResponse](t, do(t, s, http.MethodGet, "/status", nil))
	if got.AllowedTotal != limit {
		t.Errorf("allowed_total = %d, want exactly %d", got.AllowedTotal, limit)
	}
	if got.AllowedTotal+got.BlockedTotal != 300 {
		t.Errorf("allowed+blocked = %d, want 300 (a decision was lost)",
			got.AllowedTotal+got.BlockedTotal)
	}
}

// do2 is the goroutine-safe form of do: it takes no *testing.T, since the
// testing API must not be used from non-test goroutines.
func do2(s *Server, body any) {
	encoded, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/check", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	s.Handler().ServeHTTP(httptest.NewRecorder(), req)
}
