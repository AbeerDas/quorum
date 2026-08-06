# quorumgate

A fault-tolerant distributed rate limiter in Go, using a hand-written Raft consensus protocol to replicate state across three nodes with automatic leader election and failover.

> **Status:** Stage 0 (repo scaffold). See [`PRD.md`](PRD.md) for the full build spec and stage plan.

## Architecture

_Architecture diagram goes here — added once the cluster (Stage 3+) exists._

## Correctness

The Raft implementation is hand-written, so these five safety properties are proven by named, automated tests rather than asserted:

- [x] Exactly one leader is elected in a healthy 3-node cluster — `TestExactlyOneLeaderIsElectedInHealthyCluster`
- [x] A follower rejects direct write attempts — `TestFollowerRejectsDirectWrites`
- [x] A committed entry survives leader failure — `TestCommittedEntrySurvivesLeaderFailure`
- [x] A minority partition cannot elect a leader — `TestMinorityPartitionCannotElectLeader`
- [x] Replicated state machines remain identical across all nodes — `TestReplicatedStateMachinesRemainIdentical`

Nine further tests cover properties these five leave open, including Leader Completeness (a node that missed committed entries cannot win an election), Figure 8's restriction on committing entries from earlier terms, and log repair after a partition heals.

**These tests are themselves verified by mutation testing.** Each safety rule in the implementation was deliberately broken in turn to confirm a test fails. The first pass caught only 2 of 6 planted violations while every test still passed — the suite was covering the happy path and clean failures, never genuine log divergence. The gaps it exposed are what the additional tests were written to close, and the suite now catches 6 of 6.

The correctness tests run against an in-memory network so links can be severed precisely and instantly. A separate suite (`raft/grpctransport`) proves real nodes elect a leader, replicate, and fail over across actual TCP connections.

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
