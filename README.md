# quorumgate

A fault-tolerant distributed rate limiter in Go, using a hand-written Raft consensus protocol to replicate state across three nodes with automatic leader election and failover.

> **Status:** the cluster is complete and runs from a single command. Consensus, replicated
> rate limiting, observability, containers, fault injection and benchmarks are built and
> tested; the React dashboard (Stage 7) is not. See [`PRD.md`](PRD.md) for the full build
> spec and stage plan, and [`explainers/`](explainers/README.md) for plain-language write-ups
> of what was built at each stage.

## Quick start

```bash
docker compose up --build
```

Three nodes come up on ports 8081, 8082 and 8083, find each other, and elect a leader in
about ten seconds. Every rate-limit decision is agreed by a majority of the cluster before
it is answered.

```bash
# Ask any node; a follower forwards the request to the leader.
curl -s localhost:8081/check -d '{"caller_id":"alice"}'
# {"allowed":true,"remaining":499}

# Who is the leader right now?
curl -s localhost:8081/status | python3 -m json.tool
```

To check the cluster really does what this README claims, run the 18-check validation
against it — it forms a cluster, replicates, loses its leader, and recovers:

```bash
./scripts/validate-cluster.sh
```

## Architecture

_Architecture diagram goes here — added with the dashboard in Stage 7._

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

## Breaking it on purpose

The cluster ships with controls that inject real failures, so "it survives failure" can be
checked rather than taken on trust. They are **off unless the node is started with
`-demo-controls`** — they let any caller stop a node, with no authentication — and
`docker compose` enables them because nothing is published beyond the local machine.

| Control | Simulates | What the cluster does |
|---|---|---|
| `POST /admin/kill` | a crashed machine | peers fail instantly, a new leader is elected |
| `POST /admin/pause` | a hung process | peers wait out their own timeouts before reacting |
| `POST /admin/delay` | a slow network | still a member, but elections get harder |
| `POST /admin/revive` | recovery | rejoins as a follower and catches up |
| `POST /swarm` | client traffic | built-in load generator, no external tool needed |

```bash
# Watch one greedy caller get cut off while polite ones are untouched.
curl -s localhost:8081/swarm -d '{"rate":400,"duration_ms":3000,"caller_mix":"one_abusive"}'

# Kill whichever node is currently the leader, then watch the handoff.
curl -s localhost:8081/admin/kill
curl -s localhost:8082/status | python3 -m json.tool
```

A faulted node is frozen rather than disconnected: **its clock stops**. Cutting a node's
network instead would leave it holding elections against unreachable peers, raising its term
every time, so reviving it after a minute away would force a pointless election on a healthy
cluster. Measured, before the fix: a node away for 2 seconds returned at term 10 against a
cluster still at term 1. Because Raft takes its clock as an injectable dependency, the entire
fault layer sits outside the consensus code and required no change to it.

## Repo layout

- `/raft` — hand-rolled Raft consensus core
- `/limiter` — token-bucket limiter and in-memory store
- `/api` — external REST API, demo controls, and the built-in load generator
- `/cluster` — the replicated state machine that puts the limiter behind Raft
- `/fault` — failure injection: crash, hang, and network delay
- `/metrics` — Prometheus instrumentation
- `/scripts` — `validate-cluster.sh`, the live cluster check
- `/bench` — the benchmark harness behind the numbers above
- `/ui` — React/Vite dashboard (Stage 7, not yet built)

## Development

Requires Go 1.22+.

```
go build ./...
go test ./...
go test -race ./...
```

The Docker build pins Go to 1.22 and forbids the toolchain from silently upgrading itself
(`GOTOOLCHAIN=local`), so a successful image build is real evidence that the declared version
floor holds — a passing CI run alone is not, as [the build journal](.claude/skills/build-journal/SKILL.md)
records.

See [`PRD.md`](PRD.md) for the complete build spec, API contract, and mandatory stage gates.
