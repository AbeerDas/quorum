// Command quorumd runs a single quorumgate rate-limiter node.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbeerDas/quorum/api"
	"github.com/AbeerDas/quorum/limiter"
)

// sweepThreshold is how many callers may be tracked before the limiter looks
// for idle ones to drop. Sweeping is cheap but not free, so it is amortised
// rather than run on every request.
const sweepThreshold = 10_000

func main() {
	var (
		addr    = flag.String("addr", ":8080", "address to listen on")
		nodeID  = flag.String("node-id", "node-1", "identifier for this node, reported by /status")
		limit   = flag.Int("limit", 100, "requests each caller may make per window")
		window  = flag.Duration("window", time.Minute, "how long a caller takes to refill from empty to the limit")
		idleTTL = flag.Duration("idle-ttl", 10*time.Minute, "how long a fully refilled, idle caller is kept before being dropped")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *limit <= 0 || *window <= 0 {
		logger.Error("invalid configuration", "limit", *limit, "window", window.String())
		os.Exit(2)
	}

	rateLimiter := limiter.New(limiter.Config{
		Limit:          *limit,
		Window:         *window,
		IdleTTL:        *idleTTL,
		SweepThreshold: sweepThreshold,
	})

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: api.NewServer(rateLimiter, *nodeID, time.Now).Handler(),
		// Without this a slow client can hold a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("node listening",
			"addr", *addr,
			"node_id", *nodeID,
			"mode", "single-node",
			"limit", *limit,
			"window", window.String(),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
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
}
