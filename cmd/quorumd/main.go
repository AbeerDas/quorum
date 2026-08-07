// Command quorumd runs one quorumgate rate-limiter node.
//
// With no -peers it runs standalone, holding the limiter directly. Given peers
// it joins a Raft cluster, and every rate-limit decision is replicated before
// it is answered.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/AbeerDas/quorum/api"
	"github.com/AbeerDas/quorum/cluster"
	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/metrics"
	"github.com/AbeerDas/quorum/raft"
	"github.com/AbeerDas/quorum/raft/grpctransport"
)

// sweepThreshold is how many callers may be tracked before the limiter looks
// for idle ones to drop. Sweeping is cheap but not free, so it is amortised
// rather than run on every request.
const sweepThreshold = 10_000

// peer is one other node: where to reach its Raft port and its HTTP API.
type peer struct {
	id       raft.NodeID
	raftAddr string
	apiAddr  string
}

// parsePeers reads the -peers flag: "id=raftHost:port=apiHost:port,..."
func parsePeers(raw string) ([]peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var peers []peer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, "=")
		if len(parts) != 3 {
			return nil, fmt.Errorf("peer %q must look like id=raftHost:port=apiHost:port", entry)
		}
		peers = append(peers, peer{
			id:       raft.NodeID(parts[0]),
			raftAddr: parts[1],
			apiAddr:  parts[2],
		})
	}
	return peers, nil
}

func main() {
	var (
		addr     = flag.String("addr", ":8080", "address to serve the HTTP API on")
		raftAddr = flag.String("raft-addr", "", "address to serve Raft RPCs on; required when -peers is set")
		nodeID   = flag.String("node-id", "node-1", "identifier for this node, reported by /status")
		peersRaw = flag.String("peers", "", "other nodes as id=raftHost:port=apiHost:port, comma separated; empty runs standalone")

		limit   = flag.Int("limit", 100, "requests each caller may make per window")
		window  = flag.Duration("window", time.Minute, "how long a caller takes to refill from empty to the limit")
		idleTTL = flag.Duration("idle-ttl", 10*time.Minute, "how long a fully refilled, idle caller is kept before being dropped")

		electionMin   = flag.Duration("election-timeout-min", 300*time.Millisecond, "shortest wait before a follower calls an election")
		electionMax   = flag.Duration("election-timeout-max", 600*time.Millisecond, "longest wait before a follower calls an election")
		heartbeat     = flag.Duration("heartbeat", 75*time.Millisecond, "how often the leader heartbeats followers")
		failoverGrace = flag.Duration("failover-grace", time.Second, "how long a request keeps trying to reach a leader during an election")
		latencyWindow = flag.Duration("latency-window", 10*time.Second, "how far back the latency percentiles on /status look")
		logLevel      = flag.String("log-level", "info", "debug, info, warn or error; debug logs every replication round")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	collector := metrics.New(metrics.Options{
		NodeID:       *nodeID,
		RecentWindow: *latencyWindow,
	})

	if *limit <= 0 || *window <= 0 {
		logger.Error("invalid configuration", "limit", *limit, "window", window.String())
		os.Exit(2)
	}

	peers, err := parsePeers(*peersRaw)
	if err != nil {
		logger.Error("invalid -peers", "error", err)
		os.Exit(2)
	}

	rateLimiter := limiter.New(limiter.Config{
		Limit:          *limit,
		Window:         *window,
		IdleTTL:        *idleTTL,
		SweepThreshold: sweepThreshold,
	})

	var (
		backend    api.Backend
		raftNode   *raft.Node
		grpcServer *grpc.Server
		transport  *grpctransport.Transport
	)

	if len(peers) == 0 {
		backend = api.NewSingleNodeBackend(rateLimiter, *nodeID)
		logger.Info("starting standalone", "node_id", *nodeID, "mode", "single-node")
	} else {
		if *raftAddr == "" {
			logger.Error("-raft-addr is required when -peers is set")
			os.Exit(2)
		}

		raftAddrs := map[raft.NodeID]string{}
		apiAddrs := map[raft.NodeID]string{}
		var peerIDs []raft.NodeID
		for _, p := range peers {
			peerIDs = append(peerIDs, p.id)
			raftAddrs[p.id] = p.raftAddr
			apiAddrs[p.id] = p.apiAddr
		}

		transport = grpctransport.New(raftAddrs)
		fsm := cluster.NewFSM(rateLimiter)
		raftNode = raft.NewNode(raft.Config{
			ID:                 raft.NodeID(*nodeID),
			Peers:              peerIDs,
			Transport:          transport,
			StateMachine:       fsm,
			ElectionTimeoutMin: *electionMin,
			ElectionTimeoutMax: *electionMax,
			HeartbeatInterval:  *heartbeat,
			Logger:             logger,
			Metrics:            collector,
		})

		lis, err := net.Listen("tcp", *raftAddr)
		if err != nil {
			logger.Error("cannot listen for Raft traffic", "addr", *raftAddr, "error", err)
			os.Exit(1)
		}
		grpcServer = grpc.NewServer()
		grpctransport.Register(grpcServer, raftNode)
		go func() {
			if err := grpcServer.Serve(lis); err != nil {
				logger.Error("raft transport stopped", "error", err)
			}
		}()

		raftNode.Start()
		backend = api.NewClusterBackend(cluster.NewService(raftNode, fsm), apiAddrs, 3*(*heartbeat))

		logger.Info("joining cluster",
			"node_id", *nodeID, "mode", "cluster",
			"raft_addr", *raftAddr, "peers", len(peers))
	}

	httpServer := &http.Server{
		Addr: *addr,
		Handler: api.NewServer(api.ServerConfig{
			Backend:       backend,
			NodeID:        *nodeID,
			Now:           time.Now,
			FailoverGrace: *failoverGrace,
			Metrics:       collector,
			LatencyWindow: *latencyWindow,
			Logger:        logger,
		}).Handler(),
		// Without this a slow client can hold a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("api listening", "addr", *addr, "node_id", *nodeID, "limit", *limit, "window", window.String())
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()

	// Give in-flight requests a chance to finish rather than cutting them off.
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	if raftNode != nil {
		raftNode.Stop()
	}
	if grpcServer != nil {
		grpcServer.Stop()
	}
	if transport != nil {
		_ = transport.Close()
	}
}
