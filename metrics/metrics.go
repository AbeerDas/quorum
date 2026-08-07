// Package metrics collects the measurements listed in PRD.md Section 10 and
// exposes them in Prometheus text format.
//
// It is the only package that knows Prometheus exists. The consensus core
// reports through a small interface it defines itself, so the parts that must
// stay correct do not depend on a monitoring library.
package metrics

import (
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AbeerDas/quorum/raft"
)

// Outcome labels a rate-limit decision.
type Outcome string

const (
	Allowed Outcome = "allowed"
	Blocked Outcome = "blocked"
)

// RejectReason says why a request was refused, distinguishing a caller that
// genuinely exceeded its budget from one that merely arrived at the wrong node.
// Only the first is a real rejection; conflating them would make routing churn
// look like abuse.
type RejectReason string

const (
	ReasonOverLimit RejectReason = "over_limit"
	ReasonWrongNode RejectReason = "wrong_node"
	ReasonNoLeader  RejectReason = "no_leader"
)

// Collector holds every metric for one node.
type Collector struct {
	registry *prometheus.Registry
	nodeID   string

	electionDuration *prometheus.HistogramVec
	elections        *prometheus.CounterVec
	replicationLag   *prometheus.HistogramVec
	commitIndex      prometheus.Gauge
	role             *prometheus.GaugeVec
	peerHealthy      *prometheus.GaugeVec

	requestLatency *prometheus.HistogramVec
	requests       *prometheus.CounterVec
	rejections     *prometheus.CounterVec

	// recent keeps the latest latencies so /status can report percentiles over
	// current activity. Prometheus histograms answer that through a query
	// engine, which the dashboard does not have.
	recent *window

	now func() time.Time
}

// Options configures a Collector.
type Options struct {
	NodeID string
	// RecentWindow is how far back the /status percentiles look. Rolling rather
	// than cumulative, so a failover spike is visible and then recovers instead
	// of permanently skewing the number.
	RecentWindow time.Duration
	Now          func() time.Time
}

// New creates a Collector with its own registry, so two nodes in one process
// (as in tests) do not collide.
func New(opts Options) *Collector {
	if opts.RecentWindow <= 0 {
		opts.RecentWindow = 10 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	labels := prometheus.Labels{"node_id": opts.NodeID}
	factory := prometheus.NewRegistry()

	c := &Collector{
		registry: factory,
		nodeID:   opts.NodeID,
		now:      opts.Now,
		recent:   newWindow(opts.RecentWindow, opts.Now),

		electionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "quorum_raft_election_duration_seconds",
			Help:        "Time from becoming a candidate to the election being settled.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.005, 2, 12),
		}, []string{"outcome"}),

		elections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "quorum_raft_elections_total",
			Help:        "Elections this node has taken part in, by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),

		replicationLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "quorum_raft_replication_lag_seconds",
			Help:        "Time from the leader committing an entry to a follower applying it.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.001, 2, 12),
		}, []string{"peer"}),

		commitIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "quorum_raft_commit_index",
			Help:        "Highest log position known to be safely replicated.",
			ConstLabels: labels,
		}),

		role: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "quorum_raft_role",
			Help:        "1 for the node's current role, 0 for the others.",
			ConstLabels: labels,
		}, []string{"role"}),

		peerHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "quorum_peer_healthy",
			Help:        "1 when a peer answered recently, 0 when it has gone quiet.",
			ConstLabels: labels,
		}, []string{"peer"}),

		requestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "quorum_request_duration_seconds",
			Help:        "End-to-end time to decide a rate-limit request.",
			ConstLabels: labels,
			Buckets:     prometheus.ExponentialBuckets(0.0001, 2, 16),
		}, []string{"outcome"}),

		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "quorum_requests_total",
			Help:        "Rate-limit decisions made, by outcome.",
			ConstLabels: labels,
		}, []string{"outcome"}),

		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "quorum_rejected_requests_total",
			Help:        "Requests refused, by reason.",
			ConstLabels: labels,
		}, []string{"reason"}),
	}

	factory.MustRegister(
		c.electionDuration, c.elections, c.replicationLag, c.commitIndex,
		c.role, c.peerHealthy, c.requestLatency, c.requests, c.rejections,
	)

	// Publish every role up front so the series exist before the first
	// election, rather than appearing only once something happens.
	for _, r := range []raft.Role{raft.Follower, raft.Candidate, raft.Leader} {
		c.role.WithLabelValues(r.String()).Set(0)
	}
	c.role.WithLabelValues(raft.Follower.String()).Set(1)

	return c
}

// Handler serves the metrics in Prometheus text format.
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry, for tests.
func (c *Collector) Registry() *prometheus.Registry { return c.registry }

// --- raft.Metrics ---

func (c *Collector) ElectionSettled(_ uint64, took time.Duration, won bool) {
	outcome := "lost"
	if won {
		outcome = "won"
	}
	c.electionDuration.WithLabelValues(outcome).Observe(took.Seconds())
	c.elections.WithLabelValues(outcome).Inc()
}

func (c *Collector) RoleChanged(role raft.Role, _ uint64) {
	for _, r := range []raft.Role{raft.Follower, raft.Candidate, raft.Leader} {
		value := 0.0
		if r == role {
			value = 1.0
		}
		c.role.WithLabelValues(r.String()).Set(value)
	}
}

func (c *Collector) ReplicationLag(peer raft.NodeID, lag time.Duration) {
	c.replicationLag.WithLabelValues(string(peer)).Observe(lag.Seconds())
}

func (c *Collector) CommitIndexAdvanced(index uint64) {
	c.commitIndex.Set(float64(index))
}

// --- request-side measurements ---

// ObserveRequest records one completed rate-limit decision.
func (c *Collector) ObserveRequest(outcome Outcome, took time.Duration) {
	c.requestLatency.WithLabelValues(string(outcome)).Observe(took.Seconds())
	c.requests.WithLabelValues(string(outcome)).Inc()
	c.recent.add(took)

	if outcome == Blocked {
		c.rejections.WithLabelValues(string(ReasonOverLimit)).Inc()
	}
}

// ObserveRejection records a request refused for a reason other than the
// caller's own budget.
func (c *Collector) ObserveRejection(reason RejectReason) {
	c.rejections.WithLabelValues(string(reason)).Inc()
}

// SetPeerHealthy publishes whether a peer is currently answering.
func (c *Collector) SetPeerHealthy(peer raft.NodeID, healthy bool) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	c.peerHealthy.WithLabelValues(string(peer)).Set(value)
}

// Percentiles are recent request latencies, for /status and the dashboard.
type Percentiles struct {
	P50     time.Duration `json:"p50_ms_source"`
	P95     time.Duration `json:"p95_ms_source"`
	P99     time.Duration `json:"p99_ms_source"`
	Samples int           `json:"samples"`
}

// RecentLatency reports percentiles over the rolling window.
func (c *Collector) RecentLatency() Percentiles { return c.recent.percentiles() }

// window keeps recent samples with their timestamps, dropping anything older
// than the configured span.
type window struct {
	mu      sync.Mutex
	span    time.Duration
	now     func() time.Time
	at      []time.Time
	samples []time.Duration
}

func newWindow(span time.Duration, now func() time.Time) *window {
	return &window{span: span, now: now}
}

func (w *window) add(sample time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := w.now()
	w.at = append(w.at, now)
	w.samples = append(w.samples, sample)
	w.pruneLocked(now)
}

// pruneLocked drops samples that have aged out. Timestamps only ever increase,
// so the expired ones are always a prefix.
func (w *window) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.span)
	drop := 0
	for drop < len(w.at) && w.at[drop].Before(cutoff) {
		drop++
	}
	if drop == 0 {
		return
	}
	w.at = append(w.at[:0], w.at[drop:]...)
	w.samples = append(w.samples[:0], w.samples[drop:]...)
}

func (w *window) percentiles() Percentiles {
	w.mu.Lock()
	w.pruneLocked(w.now())
	sorted := append([]time.Duration(nil), w.samples...)
	w.mu.Unlock()

	if len(sorted) == 0 {
		return Percentiles{}
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return Percentiles{
		P50:     quantile(sorted, 0.50),
		P95:     quantile(sorted, 0.95),
		P99:     quantile(sorted, 0.99),
		Samples: len(sorted),
	}
}

// quantile picks the nearest-rank sample, which needs no interpolation and
// always returns a latency that actually occurred.
func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(q * float64(len(sorted)))
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}
