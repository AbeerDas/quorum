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

Every figure below was measured with [`bench/run.sh`](bench/run.sh) and is reproducible. Nothing here is estimated.

**Methodology.** [vegeta](https://github.com/tsenart/vegeta) at a fixed rate for 30 seconds per run, against a rate limit high enough that no request is ever refused, so the numbers measure throughput rather than rejection. Raft configured with a 100ms heartbeat and a 1-2s election timeout. Measured on an **Apple M2 (8 cores, 8 GB, macOS)** with the load generator running on the same machine as the servers, which caps the achievable rate — treat these as a floor.

### 1. Baseline: single node, no consensus

| Load | Throughput | p50 | p95 | p99 | Success |
|---|---|---|---|---|---|
| Unlimited rate | **41,362 req/s** | 1.56 ms | 17.15 ms | 39.39 ms | 100% |
| Fixed 6,000 req/s | 6,000 req/s | 0.09 ms | 0.15 ms | 2.99 ms | 100% |

### 2. Healthy 3-node cluster

Every request is agreed by a majority before it is answered.

| Load | Throughput | p50 | p95 | p99 | Success |
|---|---|---|---|---|---|
| Fixed 6,000 req/s | 6,000 req/s | 0.18 ms | 0.78 ms | 5.94 ms | 100% |
| Fixed 8,000 req/s | 8,000 req/s | 0.23 ms | 4.70 ms | 21.67 ms | 100% |
| Fixed 10,000 req/s | **9,999 req/s** | 0.38 ms | 9.09 ms | 25.04 ms | 100% |

**Cost of consensus:** at the same 6,000 req/s, replication roughly doubles latency (p50 0.09 → 0.18 ms, p99 2.99 → 5.94 ms). That is the price of an answer that survives the node which gave it.

### 3. Cluster under induced leader failure

3,000 req/s for 30 seconds, with the leader process killed at the 15-second mark. Load is deliberately sent to a **follower**, so the forwarding path is exercised too.

| Measure | Result |
|---|---|
| Successful requests | **89,990 of 90,000 (100.0%)** |
| Failed requests | 10 (0.011%) |
| Time serving zero requests | **0 ms** |
| p50 / p95 / p99 | 0.31 ms / 11.26 ms / 22.59 ms |

**Zero lost, zero double-counted — verified rather than asserted.** Every accepted request becomes exactly one entry in the replicated log, so the log's committed position is an independent audit of the request count. Both surviving nodes finished at commit index **89,990**, exactly matching the 89,990 successful requests: nothing was lost, nothing was counted twice, and the two survivors agreed exactly.

### A tuning finding worth recording

The node's default timeouts (75 ms heartbeat, 300-600 ms election) are tuned to make failover visibly fast in a demo. Under **sustained** load of 3,000 req/s they are too aggressive: the leader's heartbeats get starved, followers call elections against a healthy leader, and the resulting churn collapsed the cluster after about 27 seconds. The calmer values above are stable indefinitely at 10,000 req/s.

Related: the API's failover grace must exceed the election timeout. When it did not, in-flight requests aged out during a failover and returned errors; once raised above it, the same failover cost 10 requests out of 90,000.

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
