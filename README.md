# quorumgate

A fault-tolerant distributed rate limiter in Go, using a hand-written Raft consensus protocol to replicate state across three nodes with automatic leader election and failover.

> **Status:** Stage 0 (repo scaffold). See [`PRD.md`](PRD.md) for the full build spec and stage plan.

## Architecture

_Architecture diagram goes here — added once the cluster (Stage 3+) exists._

## Correctness

_The five Raft correctness tests (see [`PRD.md`](PRD.md) Section 9) will be listed here with checkmarks once Stage 3 is complete._

## Performance

_Real, measured benchmark numbers (see [`PRD.md`](PRD.md) Section 11) go here once Stage 8 is complete. No placeholder figures._

## Repo layout

- `/raft` — hand-rolled Raft consensus core
- `/limiter` — token-bucket limiter and in-memory store
- `/api` — external REST API
- `/ui` — React/Vite dashboard

## Development

Requires Go 1.22+.

```
go build ./...
go test ./...
go test -race ./...
```

See [`PRD.md`](PRD.md) for the complete build spec, API contract, and mandatory stage gates.
