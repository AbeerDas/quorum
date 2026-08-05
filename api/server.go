package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/AbeerDas/quorum/limiter"
)

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

// peerStatus describes one other node in the cluster. No peers exist until Raft
// lands in Stage 3; the shape is fixed now so the dashboard does not have to be
// rewritten when they appear.
type peerStatus struct {
	NodeID     string `json:"node_id"`
	Address    string `json:"address"`
	Healthy    bool   `json:"healthy"`
	LastSeenMS int64  `json:"last_seen_ms"`
}

// statusResponse is the body of GET /status.
type statusResponse struct {
	NodeID string `json:"node_id"`
	// Mode is "single-node" until a cluster exists, so no reader mistakes this
	// for an elected leader.
	Mode  string       `json:"mode"`
	Role  string       `json:"role"`
	Term  uint64       `json:"term"`
	Peers []peerStatus `json:"peers"`

	Limit    int   `json:"limit"`
	WindowMS int64 `json:"window_ms"`

	TrackedCallers int    `json:"tracked_callers"`
	AllowedTotal   uint64 `json:"allowed_total"`
	BlockedTotal   uint64 `json:"blocked_total"`
	UptimeMS       int64  `json:"uptime_ms"`
}

// configRequest is the body of PUT /config.
type configRequest struct {
	Limit    int   `json:"limit"`
	WindowMS int64 `json:"window_ms"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Server exposes the limiter over the REST API described in PRD.md Section 13.
//
// This is the single-node form: there is no cluster, so /status reports mode
// "single-node" rather than implying an election has taken place.
type Server struct {
	limiter *limiter.Limiter
	nodeID  string

	// now supplies the current instant. The limiter never reads a clock itself,
	// so time enters the system here and nowhere else, which keeps handlers
	// testable without sleeping.
	now     func() time.Time
	started time.Time

	allowed atomic.Uint64
	blocked atomic.Uint64

	mux *http.ServeMux
}

// NewServer wires l behind the REST API. A nil now defaults to time.Now.
func NewServer(l *limiter.Limiter, nodeID string, now func() time.Time) *Server {
	if now == nil {
		now = time.Now
	}

	s := &Server{
		limiter: l,
		nodeID:  nodeID,
		now:     now,
		started: now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/check", s.handleCheck)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/config", s.handleConfig)
	s.mux = mux

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

	var req checkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "body must be JSON of the form {\"caller_id\": \"...\"}")
		return
	}
	if req.CallerID == "" {
		s.writeError(w, http.StatusBadRequest, "caller_id is required and must not be empty")
		return
	}

	d := s.limiter.Allow(req.CallerID, s.now())

	if d.Allowed {
		s.allowed.Add(1)
		s.writeJSON(w, http.StatusOK, checkResponse{Allowed: true, Remaining: d.Remaining})
		return
	}

	s.blocked.Add(1)

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

// handleStatus reports this node's health and current limit.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use GET")
		return
	}

	cfg := s.limiter.Config()

	s.writeJSON(w, http.StatusOK, statusResponse{
		NodeID: s.nodeID,
		Mode:   "single-node",
		// The only node in the system is by definition the one accepting writes.
		// Mode above makes clear this was not won in an election.
		Role: "leader",
		// No election has happened, so the term is genuinely zero.
		Term:  0,
		Peers: []peerStatus{},

		Limit:    cfg.Limit,
		WindowMS: cfg.Window.Milliseconds(),

		TrackedCallers: s.limiter.Len(),
		AllowedTotal:   s.allowed.Load(),
		BlockedTotal:   s.blocked.Load(),
		UptimeMS:       s.now().Sub(s.started).Milliseconds(),
	})
}

// handleConfig changes the limit while the server is running.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use PUT")
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

	// Start from the live config so unrelated settings, such as eviction, are
	// carried over rather than silently reset to their zero values.
	cfg := s.limiter.Config()
	cfg.Limit = req.Limit
	cfg.Window = time.Duration(req.WindowMS) * time.Millisecond
	s.limiter.SetConfig(cfg)

	s.writeJSON(w, http.StatusOK, configRequest{
		Limit:    cfg.Limit,
		WindowMS: cfg.Window.Milliseconds(),
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
