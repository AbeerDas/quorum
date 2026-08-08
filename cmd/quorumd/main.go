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
	"github.com/AbeerDas/quorum/fault"
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

// probeHealth asks this node's own API whether it is serving.
//
// It exists so the container image can stay minimal: a distroless image has no
// shell and no curl, so the health check is the same binary invoked a second
// way rather than a tool the image would otherwise have to carry.
func probeHealth(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("cannot read address %q: %w", addr, err)
	}
	// A server bound to every interface is still reached over the loopback.
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("/status returned %s", resp.Status)
	}
	return nil
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

		demoControls = flag.Bool("demo-controls", false,
			"expose /swarm and /admin/* so the dashboard can inject faults; lets any caller stop this node, never enable in production")
		healthcheck = flag.Bool("healthcheck", false,
			"probe this node's own /status and exit 0 if healthy, for container health checks")
	)
	flag.Parse()

	// Runs as a one-shot probe rather than a server, so the container image
	// needs no shell or curl of its own.
	if *healthcheck {
		if err := probeHealth(*addr); err != nil {
			fmt.Fprintln(os.Stderr, "healthcheck:", err)
			os.Exit(1)
		}
		return
	}

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

	// The injector exists only when the demo controls are switched on. When it
	// is nil, Raft runs on the system clock over a plain transport and there is
	// no code path by which a fault could be introduced at all.
	var faults *fault.Injector
	if *demoControls {
		faults = fault.New()
		defer faults.Close()
		logger.Warn("demo controls enabled",
			"endpoints", "/swarm, /admin/kill, /admin/pause, /admin/delay, /admin/revive",
			"warning", "any caller can stop this node")
	}

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

		// With faults enabled the node reaches its peers through the injector
		// and reads the injector's clock. Both are pass-through until somebody
		// asks for a fault, and the consensus code cannot tell the difference.
		var (
			raftTransport raft.Transport = transport
			raftClock     raft.Clock
		)
		if faults != nil {
			raftTransport = fault.NewTransport(transport, faults)
			raftClock = faults.Clock()
		}

		raftNode = raft.NewNode(raft.Config{
			ID:                 raft.NodeID(*nodeID),
			Peers:              peerIDs,
			Transport:          raftTransport,
			StateMachine:       fsm,
			Clock:              raftClock,
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
		// The interceptor is the receiving half of the same fault: it decides
		// whether an arriving RPC is answered, refused, or left hanging.
		var serverOpts []grpc.ServerOption
		if faults != nil {
			serverOpts = append(serverOpts, grpc.UnaryInterceptor(faults.UnaryServerInterceptor()))
		}
		grpcServer = grpc.NewServer(serverOpts...)
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

	apiServer := api.NewServer(api.ServerConfig{
		Backend:       backend,
		NodeID:        *nodeID,
		Now:           time.Now,
		FailoverGrace: *failoverGrace,
		Metrics:       collector,
		LatencyWindow: *latencyWindow,
		Faults:        faults,
		Logger:        logger,
	})

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: apiServer.Handler(),
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

	// Stop generating load before closing the door on it, or the last requests
	// are fired at a server that is already shutting down.
	logger.Info("shutting down")
	apiServer.StopSwarm()

	// Give in-flight requests a chance to finish rather than cutting them off.
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
