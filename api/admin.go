package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/AbeerDas/quorum/fault"
)

// The endpoints in this file exist so the dashboard can break the cluster on
// purpose without anyone opening a terminal (PRD.md Section 8). They let an
// unauthenticated caller stop a node, so they are registered only when the
// node is started with -demo-controls, which is off by default.

// roleDown is the role reported by a node that has been stopped on purpose. It
// is deliberately not one of raft's roles: Raft has no notion of a node being
// switched off, and this is the API's description of the machine rather than
// the consensus state inside it.
const roleDown = "down"

// adminResponse is the reply to every fault control: what the node is now.
type adminResponse struct {
	NodeID string      `json:"node_id"`
	Fault  fault.State `json:"fault"`
}

// delayRequest is the body of POST /admin/delay.
type delayRequest struct {
	DelayMS int64 `json:"delay_ms"`
}

// registerDemoControls wires the fault and load-generator endpoints.
func (s *Server) registerDemoControls(mux *http.ServeMux) {
	mux.HandleFunc("/admin/kill", s.faultHandler(s.faults.Kill))
	mux.HandleFunc("/admin/pause", s.faultHandler(s.faults.Pause))
	mux.HandleFunc("/admin/revive", s.faultHandler(s.faults.Revive))
	mux.HandleFunc("/admin/delay", s.handleDelay)
	mux.HandleFunc("/swarm", s.handleSwarm)
}

// faultHandler turns one injector action into an endpoint.
func (s *Server) faultHandler(apply func()) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use POST")
			return
		}

		apply()
		state := s.faults.State()

		// Logged at warn: a node going down on purpose still needs to be
		// obvious in the logs, or a demo fault looks like a real outage to
		// whoever reads them afterwards.
		s.logger.Warn("demo fault applied", "control", r.URL.Path, "mode", string(state.Mode))

		s.writeJSON(w, http.StatusOK, adminResponse{NodeID: s.nodeID, Fault: state})
	}
}

// handleDelay slows this node's Raft traffic without removing it from the
// cluster, so a viewer can find the point where elections start to struggle.
func (s *Server) handleDelay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use POST")
		return
	}

	var req delayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "body must be JSON of the form {\"delay_ms\": 100}")
		return
	}
	if req.DelayMS < 0 {
		s.writeError(w, http.StatusBadRequest, "delay_ms must not be negative")
		return
	}

	s.faults.SetDelay(time.Duration(req.DelayMS) * time.Millisecond)
	state := s.faults.State()
	s.logger.Warn("demo fault applied", "control", r.URL.Path, "delay_ms", state.DelayMS)

	s.writeJSON(w, http.StatusOK, adminResponse{NodeID: s.nodeID, Fault: state})
}

// faultState reports this node's fault condition. A node without demo controls
// is simply healthy - there is nothing that could have made it otherwise.
func (s *Server) faultState() fault.State {
	if s.faults == nil {
		return fault.State{Mode: fault.Healthy}
	}
	return s.faults.State()
}

// down reports whether this node is pretending to be off, in which case it
// refuses to serve rate-limit decisions. A crashed machine does not answer its
// clients either, and a node shown as down in the dashboard while still
// quietly doing the work would make the whole demo a lie.
func (s *Server) down() bool {
	return s.faults != nil && s.faults.Down()
}
