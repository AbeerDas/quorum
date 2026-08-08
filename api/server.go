package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AbeerDas/quorum/fault"
	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/metrics"
	"github.com/AbeerDas/quorum/raft"
)

// leaderRouter is implemented by a backend that knows where its peers' APIs
// live, so a write that landed on a follower can be passed to the leader.
type leaderRouter interface {
	PeerAddr(id raft.NodeID) (string, bool)
}

// forwardedHeader marks a request that has already been passed on once. A
// second hop means leadership moved again mid-flight, and bouncing further
// risks a request circling the cluster instead of failing honestly.
const forwardedHeader = "X-Quorum-Forwarded"

// checkRequest is the body of POST /check.
type checkRequest struct {
	CallerID string `json:"caller_id"`
}

// checkResponse is the body of POST /check. RetryAfterMS is omitted entirely on
// an allowed request, where it would be meaningless.
type checkResponse struct {
	Allowed      bool  `json:"allowed"`
	Remaining    int   `json:"remaining"`
	RetryAfterMS int64 `json:"retry_after_ms,omitempty"`
}

// peerStatus describes one other node in the cluster.
type peerStatus struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address,omitempty"`
	Healthy bool   `json:"healthy"`
	// LastSeenMS is milliseconds since this node last heard from the peer, or
	// -1 when unknown. Only a leader contacts peers, so a follower reports -1.
	LastSeenMS int64 `json:"last_seen_ms"`
}

// statusResponse is the body of GET /status.
type statusResponse struct {
	NodeID string `json:"node_id"`
	// Mode is "single-node" until a cluster exists, so no reader mistakes a
	// lone node for an elected leader.
	Mode     string       `json:"mode"`
	Role     string       `json:"role"`
	Term     uint64       `json:"term"`
	LeaderID string       `json:"leader_id"`
	Peers    []peerStatus `json:"peers"`

	Limit    int   `json:"limit"`
	WindowMS int64 `json:"window_ms"`

	TrackedCallers int    `json:"tracked_callers"`
	AllowedTotal   uint64 `json:"allowed_total"`
	BlockedTotal   uint64 `json:"blocked_total"`
	UptimeMS       int64  `json:"uptime_ms"`

	// Fault is what has been done to this node on purpose, and DemoControls
	// says whether it could have been. Both are reported so the dashboard can
	// distinguish a node that is genuinely unreachable from one somebody
	// switched off to make a point.
	Fault        fault.State `json:"fault"`
	DemoControls bool        `json:"demo_controls"`
	Swarm        swarmStatus `json:"swarm"`

	// Latency describes recent activity only, so a failover spike shows up and
	// then recovers rather than being averaged away forever.
	Latency latencyReport `json:"latency"`
}

type latencyReport struct {
	P50MS    float64 `json:"p50_ms"`
	P95MS    float64 `json:"p95_ms"`
	P99MS    float64 `json:"p99_ms"`
	Samples  int     `json:"samples"`
	WindowMS int64   `json:"window_ms"`
}

// configRequest is the body of PUT /config.
type configRequest struct {
	Limit    int   `json:"limit"`
	WindowMS int64 `json:"window_ms"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// ServerConfig describes one node's HTTP API.
type ServerConfig struct {
	Backend Backend
	NodeID  string

	// Now supplies the instant each request is judged at. Nil means time.Now.
	Now func() time.Time

	// FailoverGrace is how long a request may keep trying to reach a leader
	// while one is being elected, before giving up and returning an error.
	// Zero disables retrying.
	FailoverGrace time.Duration

	HTTPClient *http.Client

	// Metrics collects the measurements in PRD.md Section 10. Nil disables
	// collection and leaves /metrics unregistered.
	Metrics *metrics.Collector
	// LatencyWindow is the span the /status percentiles describe.
	LatencyWindow time.Duration

	// Faults enables the demo controls in PRD.md Section 13: the fault
	// injectors and the built-in load generator. Nil leaves them unregistered,
	// which is the default - they let an unauthenticated caller stop a node.
	Faults *fault.Injector

	Logger *slog.Logger
}

// Server exposes the limiter over the REST API in PRD.md Section 13.
type Server struct {
	backend Backend
	nodeID  string

	now           func() time.Time
	started       time.Time
	failoverGrace time.Duration
	client        *http.Client

	allowed atomic.Uint64
	blocked atomic.Uint64

	metrics       *metrics.Collector
	latencyWindow time.Duration
	logger        *slog.Logger

	faults  *fault.Injector
	swarmMu sync.Mutex
	swarm   *swarmRun

	mux *http.ServeMux
}

// NewServer wires a backend behind the REST API.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.LatencyWindow <= 0 {
		cfg.LatencyWindow = 10 * time.Second
	}

	s := &Server{
		backend:       cfg.Backend,
		nodeID:        cfg.NodeID,
		now:           cfg.Now,
		started:       cfg.Now(),
		failoverGrace: cfg.FailoverGrace,
		client:        cfg.HTTPClient,
		metrics:       cfg.Metrics,
		latencyWindow: cfg.LatencyWindow,
		logger:        cfg.Logger.With("node_id", cfg.NodeID),
		faults:        cfg.Faults,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/check", s.handleCheck)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/config", s.handleConfig)
	if cfg.Metrics != nil {
		// Refresh cluster gauges on the way in, so a scrape always reflects the
		// node's view right now rather than whenever /status was last polled.
		collectorHandler := cfg.Metrics.Handler()
		mux.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.refreshClusterGauges()
			collectorHandler.ServeHTTP(w, r)
		}))
	}
	s.mux = mux

	// The swarm dispatches into the router, so the router has to exist first.
	if cfg.Faults != nil {
		s.registerDemoControls(mux)
		s.logger.Warn("demo controls enabled: /swarm and /admin/* can stop this node, do not expose them")
	}

	return s
}

// Handler returns the router serving the API.
func (s *Server) Handler() http.Handler { return s.mux }

// handleCheck decides whether one request from a caller may proceed.
func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use POST")
		return
	}
	if s.down() {
		s.writeError(w, http.StatusServiceUnavailable, "node is down (simulated fault)")
		return
	}

	var req checkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "body must be JSON of the form {\"caller_id\": \"...\"}")
		return
	}
	if req.CallerID == "" {
		s.writeError(w, http.StatusBadRequest, "caller_id is required and must not be empty")
		return
	}

	at := s.now()
	deadline := at.Add(s.failoverGrace)
	started := time.Now()

	for {
		d, err := s.backend.Check(r.Context(), req.CallerID, at)
		if err == nil {
			s.recordDecision(d, time.Since(started))
			s.writeDecision(w, d)
			return
		}

		if done := s.tryForward(w, r, err, req, deadline); done {
			return
		}
	}
}

// recordDecision measures one completed decision. Latency is taken from the
// real clock rather than the injectable one: it is a measurement of this
// machine, not part of replicated state.
func (s *Server) recordDecision(d limiter.Decision, took time.Duration) {
	if s.metrics == nil {
		return
	}
	outcome := metrics.Allowed
	if !d.Allowed {
		outcome = metrics.Blocked
	}
	s.metrics.ObserveRequest(outcome, took)
}

// handleConfig changes the limit across the cluster.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use PUT")
		return
	}
	if s.down() {
		s.writeError(w, http.StatusServiceUnavailable, "node is down (simulated fault)")
		return
	}

	var req configRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "body must be JSON of the form {\"limit\": 100, \"window_ms\": 60000}")
		return
	}
	if req.Limit <= 0 {
		s.writeError(w, http.StatusBadRequest, "limit must be greater than zero")
		return
	}
	if req.WindowMS <= 0 {
		s.writeError(w, http.StatusBadRequest, "window_ms must be greater than zero")
		return
	}

	at := s.now()
	deadline := at.Add(s.failoverGrace)
	window := time.Duration(req.WindowMS) * time.Millisecond

	for {
		err := s.backend.SetLimit(r.Context(), req.Limit, window, at)
		if err == nil {
			s.writeJSON(w, http.StatusOK, configRequest{Limit: req.Limit, WindowMS: req.WindowMS})
			return
		}

		if done := s.tryForward(w, r, err, req, deadline); done {
			return
		}
	}
}

// handleStatus reports this node's health, role, and current limit.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use GET")
		return
	}

	at := s.now()
	st := s.backend.Status(at)
	cfg := s.backend.Config()

	if st.Peers == nil {
		st.Peers = []peerStatus{}
	}

	// A frozen node's Raft state is whatever it was when it stopped, so a
	// killed leader would carry on calling itself the leader and the dashboard
	// would show two at once. Its own view is stale by definition; what is
	// true about it is that it is down (PRD.md Section 8).
	if s.down() {
		st.Role = roleDown
		st.LeaderID = ""
	}

	s.writeJSON(w, http.StatusOK, statusResponse{
		NodeID:   st.NodeID,
		Mode:     st.Mode,
		Role:     st.Role,
		Term:     st.Term,
		LeaderID: st.LeaderID,
		Peers:    st.Peers,

		Limit:    cfg.Limit,
		WindowMS: cfg.Window.Milliseconds(),

		TrackedCallers: s.backend.TrackedCallers(),
		AllowedTotal:   s.allowed.Load(),
		BlockedTotal:   s.blocked.Load(),
		UptimeMS:       at.Sub(s.started).Milliseconds(),
		Latency:        s.latencyReport(),

		Fault:        s.faultState(),
		DemoControls: s.faults != nil,
		Swarm:        s.swarmStatus(),
	})
}

// refreshClusterGauges publishes the node's current view of its peers.
func (s *Server) refreshClusterGauges() {
	if s.metrics == nil {
		return
	}
	for _, p := range s.backend.Status(s.now()).Peers {
		s.metrics.SetPeerHealthy(raft.NodeID(p.NodeID), p.Healthy)
	}
}

// latencyReport describes recent request latency for the dashboard.
func (s *Server) latencyReport() latencyReport {
	if s.metrics == nil {
		return latencyReport{WindowMS: s.latencyWindow.Milliseconds()}
	}
	p := s.metrics.RecentLatency()
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return latencyReport{
		P50MS:    ms(p.P50),
		P95MS:    ms(p.P95),
		P99MS:    ms(p.P99),
		Samples:  p.Samples,
		WindowMS: s.latencyWindow.Milliseconds(),
	}
}

// tryForward handles a backend error, passing the request to the leader when
// that is what the error calls for. It reports whether the response is finished.
//
// Returning false means "try the backend again": the only case where that
// happens is a leader that turned out to be down, which is definitively a
// request that never arrived anywhere and so is safe to send again.
func (s *Server) tryForward(w http.ResponseWriter, r *http.Request, err error, body any, deadline time.Time) bool {
	var notLeader *raft.NotLeaderError
	if !errors.As(err, &notLeader) {
		s.observeRejection(metrics.ReasonNoLeader)
		s.logger.Warn("request could not be completed", "error", err)
		s.writeError(w, http.StatusServiceUnavailable, "could not complete the request: "+err.Error())
		return true
	}
	s.observeRejection(metrics.ReasonWrongNode)

	// Already passed on once and this node still is not the leader: leadership
	// is moving faster than the request can chase it.
	if r.Header.Get(forwardedHeader) != "" {
		s.writeError(w, http.StatusServiceUnavailable, "leadership changed while the request was in flight, retry")
		return true
	}

	router, ok := s.backend.(leaderRouter)
	if !ok || notLeader.Leader == "" {
		s.writeError(w, http.StatusServiceUnavailable, "no leader is currently available, retry shortly")
		return true
	}

	addr, known := router.PeerAddr(notLeader.Leader)
	if !known {
		s.writeError(w, http.StatusServiceUnavailable, "leader "+string(notLeader.Leader)+" has no configured address")
		return true
	}

	forwardErr := s.forward(w, r, addr, body)
	if forwardErr == nil {
		return true
	}

	// The connection was refused, so the request was never delivered and
	// cannot have taken effect. That is the one failure it is safe to retry.
	// Any other failure leaves it unknown whether the leader applied it, and
	// retrying an unknown is how a request gets counted twice.
	if isDialFailure(forwardErr) && s.now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		return false
	}

	s.writeError(w, http.StatusServiceUnavailable, "could not reach the leader: "+forwardErr.Error())
	return true
}

func (s *Server) observeRejection(reason metrics.RejectReason) {
	if s.metrics != nil {
		s.metrics.ObserveRejection(reason)
	}
}

// forward sends the request on to the leader and copies its reply back, so the
// caller never has to know which node it reached.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, addr string, body any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method,
		"http://"+addr+r.URL.Path, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(forwardedHeader, "1")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if v := resp.Header.Get("Retry-After"); v != "" {
		w.Header().Set("Retry-After", v)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)

	// Count the outcome here too, so a node's totals reflect the traffic it
	// served rather than only what it decided itself.
	if resp.StatusCode == http.StatusTooManyRequests {
		s.blocked.Add(1)
	} else if resp.StatusCode == http.StatusOK && r.URL.Path == "/check" {
		s.allowed.Add(1)
	}

	_, err = io.Copy(w, resp.Body)
	return err
}

// isDialFailure reports whether the connection was never established, which
// means the request certainly did not arrive.
func isDialFailure(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}

func (s *Server) writeDecision(w http.ResponseWriter, d limiter.Decision) {
	if d.Allowed {
		s.allowed.Add(1)
		s.writeJSON(w, http.StatusOK, checkResponse{Allowed: true, Remaining: d.Remaining})
		return
	}

	s.blocked.Add(1)
	s.logger.Debug("request rejected",
		"reason", string(metrics.ReasonOverLimit),
		"retry_after_ms", d.RetryAfter.Milliseconds())

	// Round the header up to whole seconds: telling a client "retry after 0s"
	// invites an immediate retry that is certain to be refused again.
	retryAfterSec := int64(math.Ceil(d.RetryAfter.Seconds()))
	if retryAfterSec < 1 {
		retryAfterSec = 1
	}
	w.Header().Set("Retry-After", strconv.FormatInt(retryAfterSec, 10))

	s.writeJSON(w, http.StatusTooManyRequests, checkResponse{
		Allowed:      false,
		Remaining:    d.Remaining,
		RetryAfterMS: d.RetryAfter.Milliseconds(),
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, errorResponse{Error: msg})
}
