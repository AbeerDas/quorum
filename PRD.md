# Build spec: quorumgate — a distributed rate limiter

This document is written to be handed to an engineering agent as a complete build brief. It is intentionally prescriptive. Sections 1-4 explain the product in plain language. Sections 5 onward are the technical contract the agent must follow. Where this document specifies a version, filename, endpoint, or data shape, treat it as a requirement, not a suggestion. Where it says DO NOT, that boundary exists to keep the build one-shottable — respect it. Section 12 defines mandatory stage gates: the agent must not move to the next stage until the current stage's tests pass, its manual validation checklist is complete, and the working state has been committed and pushed to GitHub.

This document is the source of truth for this project. `CLAUDE.md` and any installed skills exist to help an agent follow it faithfully — if they ever conflict with this document, this document wins.

## 1. The problem (plain language)

Any service that takes requests from the outside world, an API, a login page, a payment endpoint, is vulnerable to being overwhelmed. Sometimes that is an attacker deliberately hammering it. Sometimes it is one customer's buggy script calling it too fast by accident. Either way, without something standing guard, one bad actor can degrade or crash the service for everyone else.

## 2. The goal

Build a small system that sits in front of a service and enforces a simple rule: each caller gets a fixed number of requests per minute, no more. If they go over, they are turned away cleanly instead of being allowed to pile on.

The harder goal underneath that: the guard itself must not become a new single point of failure. If the one thing enforcing the limit crashes, the protection disappears. So the real goal is a guard that keeps working even when part of it goes down, and hands off between its own copies without ever losing count.

## 3. Who this is for

This is a portfolio project, but the design target is a mid-size API with multiple paying customers on different tiers, where the business must guarantee no single customer can degrade service for others, and needs enforcement to survive individual server failures automatically, with no human intervention.

## 4. Scope boundaries (plain language)

In scope: counting requests per caller, resetting counts on a time window, cleanly blocking over-limit callers, running as a fault-tolerant cluster, automatic failover.

Out of scope: identifying or fingerprinting attackers, billing or pricing logic, per-customer live limit editing, running at massive (thousands-of-servers) scale, persistence to disk.

## 5. Positioning: this is a systems project, not a dashboard project

This is the single most important framing decision in the whole build, and it applies to the README, the repo structure, and how the project gets talked about in an interview.

The value of this project is entirely in this stack:

```
Client
  |
  HTTP
  |
Rate limiter nodes
  |
Raft consensus (hand-written)
  |
Replicated state machines
  |
Automatic failure recovery

```

The React dashboard built in Section 8 is a visualization layer on top of that stack. It exists to make the demo watchable, not because the project is a frontend project. If a reader's first impression is "rate limiting dashboard," the project has been mis-positioned.

Concrete rules that follow from this:

* The README's opening line must describe the distributed systems work, not the UI. Use something like: "A fault-tolerant distributed rate limiter in Go, using a hand-written Raft consensus protocol to replicate state across three nodes with automatic leader election and failover." Do NOT open with "a rate limiting dashboard" or lead with a UI screenshot before the architecture is explained.
* Repo root structure should put the Go implementation front and center (e.g. `/raft`, `/limiter`, `/api` at the top level) with the UI clearly scoped in its own `/ui` directory, not the other way around.
* The architecture diagram (the one already built for this project) comes before any UI screenshot in the README.
* Correctness tests (Section 9) get their own README section with checkmarks. This is the part that proves the hard work is real, and it is what separates this from "configured Redis and Kubernetes and called it distributed systems."

## 6. Architecture (technical contract)

Three identical Go server nodes form a cluster. One is the elected leader; the other two are followers holding live replicated copies of the state. The rate-limit counters live in an in-memory key-value store on each node. State changes flow through a hand-rolled Raft implementation so all nodes agree on the counts.

Two network surfaces, deliberately separated:

* Node-to-node (internal): gRPC. Raft RPCs (RequestVote, AppendEntries) travel between nodes over gRPC. This is the "interesting" distributed-systems surface.
* Client-and-UI-facing (external): plain HTTP/JSON REST. The React dashboard and any client hitting the limiter use REST. This is a deliberate decision: browsers cannot speak gRPC directly (they do not support the HTTP/2 trailers gRPC requires), and adding an Envoy or gRPC-Web proxy to bridge that gap introduces an extra moving part that endangers a one-shot build. Keeping the browser on REST removes that whole class of failure.

```
Browser (React UI) --REST/JSON--> any node's HTTP API
Load traffic       --REST/JSON--> leader's /check endpoint
Node <--gRPC (Raft: RequestVote, AppendEntries)--> Node

```

**Why hand-rolled Raft**

The user has chosen to implement Raft from scratch rather than use `hashicorp/raft`. This is the highest-value part of the project for interviews and the highest-risk part of the build. A broken hand-rolled Raft implementation is worse than no Raft implementation at all, because interviewers know Raft is hard and will probe correctness directly. The agent MUST treat Raft as its own isolated, test-first stage (Section 12, Stage 3) and MUST NOT begin any other stage until every correctness test in Section 9 passes. Reference the Raft paper's Figure 2 (the state/RPC summary) as the spec for correctness. A minimal correct implementation of leader election + log replication + safety is the target; multi-Raft, snapshotting, and log compaction are explicitly OUT of scope.

## 7. Technology and versions (pin these)

* Language: Go 1.22+ (uses `log/slog` for structured logging, and modern `context` cancellation).
* Internal RPC: gRPC via `google.golang.org/grpc`, schema in Protocol Buffers (`proto3`).
* External API: Go standard library `net/http` with `encoding/json`. No web framework.
* Consensus: hand-rolled, no consensus library. `hashicorp/raft` is explicitly NOT a dependency.
* Data store: custom in-memory map guarded by a `sync.RWMutex`. No external database, no Redis, no disk persistence.
* Observability: `log/slog` for structured logs; `prometheus/client_golang` for metrics, exposed at `/metrics` in Prometheus text format. See Section 10.
* UI: React 18 with Vite. TypeScript. Plain CSS or a single lightweight styling approach — DO NOT pull in a heavy component library. Charts via a single lightweight library (`recharts` is acceptable).
* Containerization: Docker + Docker Compose. Each node is one container; the UI is one container. `docker compose up` starts the entire system.
* Load/benchmark tool (external, for formal numbers only): `vegeta`. The UI itself also drives load internally for the live demo, so vegeta is only needed to capture the headline benchmark figures for the README (Section 11).
* Concurrency correctness: every Go stage gate (Section 12) requires `go test -race ./...` to pass, not just `go test ./...`. A distributed Go system that passes the race detector is itself a real signal and must be treated as a hard requirement, not a nice-to-have.

## 8. The UI (technical contract)

The dashboard is the demo, and it exists to make the distributed-systems work visible, not to be the point of the project (see Section 5). It must serve two audiences at once: someone learning the concept for the first time, and a developer who wants the raw numbers. Visual direction: clean and light, teaching-focused, with explanatory tooltips on every technical term.

**The UI MUST let the user trigger conditions directly (no terminal needed)**

* Start / stop a traffic swarm. A control to fire a sustained stream of requests at the cluster at a user-chosen rate (a slider, e.g. 100-5000 req/s) so the user can watch the limiter allow-then-block in real time. This load is generated by the backend on command (a `/swarm` control endpoint that spins up an internal load generator), NOT by requiring the user to run vegeta.
* Adjust the limit live. A control to set the per-caller limit and window so the user can see blocking kick in sooner or later.
* Choose caller mix. A control to simulate one abusive caller vs. many well-behaved ones, so the user can see that blocking one caller does not affect others (the core fairness point).
* Kill the leader. A button that stops the current leader node, so the user can watch a follower get elected and take over live. This is the money shot.
* Pause / freeze a node. A button that freezes a node (simulating a hung process, distinct from a clean kill) to show the cluster tolerating an unresponsive peer.
* Introduce network delay. A control to inject latency between nodes, so the user can see elections slow down and understand why timing matters in Raft.

**The UI MUST teach while it shows**

* Each of the three nodes is drawn with a live status: leader (highlighted), follower, or down. Show each node's current Raft term and its role, with a tooltip explaining what a "term" is in one plain sentence.
* A live count of allowed vs. blocked requests, updating in real time.
* A live latency readout (p50/p95/p99) so a dev sees real numbers.
* A live view of the observability metrics from Section 10 (leader election duration, replication lag, node health) surfaced somewhere visible, so the dashboard doubles as proof the system is instrumented like production infrastructure, not a student toy.
* When the leader is killed, the UI should visibly narrate the handoff ("Leader node-1 went down -> election started -> node-3 elected leader in 1.2s -> no requests dropped"), so a learner sees cause and effect, not just a status flicker.
* Every piece of jargon (term, quorum, election, follower, token bucket) gets a hover tooltip with a one-line plain-language definition.

**How the UI gets live data**

The UI polls each node's `/status` endpoint (or opens a Server-Sent Events stream from the leader if the agent judges SSE reliable within scope; polling every 500ms is an acceptable and simpler fallback — prefer the fallback if SSE risks the build). DO NOT use WebSockets — polling or SSE only, to keep it simple.

## 9. Correctness tests (the actual gold — non-negotiable)

These five tests are the proof that the hand-rolled Raft implementation is real and correct, not just present. They must exist as named, automated tests (not manual spot-checks), must all pass before Stage 3 is considered complete, and must be listed with checkmarks in the README exactly as below:

* [ ] Exactly one leader is elected in a healthy 3-node cluster
* [ ] A follower rejects direct write attempts (writes must go through the leader)
* [ ] A committed entry survives leader failure (a value written before the leader dies is still present and correct after a new leader takes over)
* [ ] A minority partition (1 of 3 nodes isolated) cannot elect a leader
* [ ] Replicated state machines remain identical across all nodes after a sequence of writes and a mid-sequence leader failure

Every one of these is a real Raft safety property, not a made-up checklist item. If any of them cannot be made to pass, that is a signal to fall back per the Section 13 risk note, not to ship anyway with a caveat.

## 10. Observability requirements

This is a required part of the build, not a stretch goal, because it is what separates "production-style distributed infrastructure" from "student implementation" to a reader skimming the repo.

* Structured logging via `log/slog` on every node, at minimum logging: leader elections (with term number and outcome), every AppendEntries round (at debug level), every rejected request, and every state transition (follower to candidate to leader).
* Metrics, exposed at `/metrics` in Prometheus text format, at minimum:
  * Leader election duration (time from election start to a new leader being confirmed)
  * Raft replication lag (time between a leader committing an entry and each follower applying it)
  * Request latency (p50/p95/p99, tagged allowed vs. blocked)
  * Rejected request count (tagged by reason: over-limit vs. wrong-node)
  * Node health / current role per node
* These metrics are what the UI surfaces live (Section 8) and what gets screenshotted or graphed in the README.

## 11. Performance and benchmarking methodology

Do not just say "it survives failure." The project must demonstrate it handles real throughput, with a stated methodology, not a single vanity number.

Required benchmark runs, each documented in the README with the actual measured numbers (never placeholder or estimated figures):

1. Baseline single-node throughput: one node, no Raft overhead, maximum sustained req/s and p50/p95/p99 latency using `vegeta` at a fixed rate for at least 30 seconds.
2. 3-node cluster throughput under normal operation: same load profile, all three nodes healthy, showing the overhead Raft replication adds compared to baseline.
3. 3-node cluster under induced failure: same load profile, with the leader killed partway through the test window, showing throughput dip and recovery time, and confirming zero lost or double-counted requests across the failure.

Example of the target shape of the writeup (numbers must be real, this is a template, not a claim to copy):

"Achieved [X] requests/sec across a healthy 3-node cluster with p99 latency of [Y]ms. Under an induced leader failure mid-load-test, the cluster elected a new leader in [Z]ms with zero dropped or double-counted requests."

## 12. Build stages (mandatory gates, the agent MUST follow this order)

Every stage below follows the same gate structure. Do not start the next stage until every box in the current stage's gate is checked. Each gate ends with a required git commit and push, so the repository's history is itself a record of working, tested checkpoints, not a single final dump.

**Stage gate template (applies to every stage):**

* [ ] All listed build items are implemented
* [ ] `go test ./...` passes
* [ ] `go test -race ./...` passes (for any Go stage)
* [ ] Manual validation checklist for the stage is complete
* [ ] Commit with a message describing the stage (e.g. `stage 3: hand-rolled raft core, all correctness tests passing`)
* [ ] Tag the commit (e.g. `v0.4-raft-core`) and push both the commit and the tag to GitHub
* [ ] Only then proceed to the next stage

**Stage 0 — Repo scaffold**

Build: install the skills in Section 18 (discovery skill and discipline layer first, then the Go skills relevant to the stages ahead), then Go module init, folder structure (`/limiter`, `/raft`, `/api`, `/ui`), `.gitignore`, empty README skeleton, LICENSE, initial CI config placeholder. Validation: `go build ./...` succeeds on an empty scaffold. Tag: `v0.1-scaffold`

**Stage 1 — In-memory token-bucket limiter (single node, no networking)**

Build: the token-bucket limiter and in-memory store, pure Go, unit-tested. Validation: limiter allows N requests then blocks, and refills correctly over time. Tag: `v0.2-limiter-core`

**Stage 2 — Single-node HTTP API**

Build: wrap Stage 1 in the REST API (`/check`, `/status`, `/config`). One node, no cluster yet. Validation: `curl` against `/check` allows then blocks; `/status` returns node health. Tag: `v0.3-single-node-api`

**Stage 3 — Hand-rolled Raft core (isolated, test-first)**

Build: leader election, log replication, and safety per Raft Figure 2, with gRPC transport between nodes, built and tested in isolation from the limiter logic. Validation: all five correctness tests in Section 9 pass. Tag: `v0.4-raft-core`

**Stage 4 — Wire the limiter state through Raft**

Build: rate-limit counter updates become Raft log entries applied to the state machine on every node. Validation: a count incremented via the leader is present on a follower after failover; kill-the-leader mid-count does not lose or double-count (verify by comparing totals before/after). Tag: `v0.5-replicated-state`

**Stage 5 — Observability layer**

Build: structured logging and the metrics in Section 10, exposed at `/metrics`. Validation: `/metrics` returns real, changing values during a live test; logs show election events with term numbers. Tag: `v0.6-observability`

**Stage 6 — Docker Compose cluster**

Build: three node containers + config so `docker compose up` yields a live 3-node cluster. Validation: cluster forms from a single command; nodes find each other. Tag: `v0.7-dockerized`

**Stage 7 — React dashboard**

Build: the UI in Section 8, against the running cluster. DO NOT start this stage until Stage 6's gate is fully checked. Validation: every control in Section 8 works against the live cluster; killing the leader from the UI shows a visible, correct handoff. Tag: `v0.8-ui`

**Stage 8 — Benchmarking**

Build: run the three benchmark scenarios in Section 11 and record real numbers. Validation: README's performance section contains actual measured figures, not placeholders, with methodology described. Tag: `v0.9-benchmarked`

**Stage 9 — README, positioning, and final polish**

Build: README leads with the framing in Section 5, includes the correctness test checklist from Section 9 with checkmarks, the architecture diagram, real benchmark numbers, a GIF of the kill-the-leader demo, LICENSE, and a short CONTRIBUTING.md. Validation: every item in Section 13's definition of done is checked. Tag: `v1.0`

## 13. API contract (external REST)

The agent may refine field names but MUST keep these endpoints and their intent:

* `POST /check` — body `{ "caller_id": "string" }` -> `200 {"allowed": true, "remaining": int}` or `429 {"allowed": false, "retry_after_ms": int}`.
* `GET /status` — -> node role (`leader`/`follower`/`candidate`), current term, known peers and their last-seen health, allowed/blocked counters, latency percentiles.
* `GET /metrics` — Prometheus text format, per Section 10.
* `PUT /config` — body `{ "limit": int, "window_ms": int }` -> updates the limit live (leader only; followers redirect).
* `POST /swarm` — body `{ "rate": int, "duration_ms": int, "caller_mix": "one_abusive" | "many_fair" }` -> starts the internal load generator for the live demo.
* `POST /admin/kill` and `POST /admin/pause` and `POST /admin/delay` — control endpoints the UI uses to inject fault conditions. These are demo affordances; gate them behind a build flag or clearly mark them non-production.

## 14. Data shapes (internal)

* Store entry: `caller_id -> { tokens float64, last_refill time.Time }` for token bucket.
* Raft log entry: `{ term uint64, index uint64, command: {type: "consume"|"config", caller_id, amount} }`.
* Keep the state machine deterministic: applying the same log on any node yields identical state. This is a correctness requirement, not a nicety.

## 15. Definition of done (final self-check, all stage tags v0.1 through v1.0 must exist in git history)

* `docker compose up` brings up a working 3-node cluster with the UI reachable in a browser.
* From the UI alone, with no terminal, a user can: start a swarm, watch allow->block, kill the leader, and watch a correct failover with zero dropped or double-counted requests.
* `go test ./...` and `go test -race ./...` both pass across the whole repo.
* All five correctness tests in Section 9 pass and are checked off in the README.
* `/metrics` exposes real, live values for every metric in Section 10.
* README leads with the distributed-systems framing from Section 5, not a UI description, and contains real benchmark numbers (never placeholders) with methodology from Section 11.
* No placeholder values (`[X]`, `TODO`, `lorem`) anywhere in shipped docs or UI.
* Git history shows a tagged, working commit for every stage in Section 12.

## 16. Risks and explicit non-goals

* Hand-rolled Raft is the likeliest thing to consume time or break. It is isolated in Stage 3 for exactly this reason, gated on all five correctness tests before anything else is built on top of it. If Raft cannot be made correct in the available time, the honest fallback is to ship through Stage 2 (a solid single-node limiter with the UI pointed at one node, clearly labeled as such) rather than ship a broken cluster with a caveat — a broken Raft implementation is worse than none.
* Non-goals, do not build: disk persistence / WAL, snapshotting, log compaction, multi-Raft, sharding, TLS/auth, a real database, Kubernetes manifests, WebSockets, a gRPC-Web/Envoy proxy. Every one of these is a scope trap for a one-shot build.

## 17. Resume bullet templates (use only once these are actually true)

Do not write these until the corresponding stage tags exist in git history with passing tests. Once Stage 9 is complete:

* "Built a fault-tolerant distributed rate limiter in Go using a hand-written Raft consensus protocol, replicating token-bucket state across 3 nodes with automatic leader election and failover"
* "Implemented gRPC-based node communication, replicated state machines, and Dockerized cluster deployment, achieving [X] req/sec throughput with p99 latency of [Y]ms" — fill in X and Y from the real Stage 8 benchmark, never estimate.

## 18. Recommended skills (install via the skills CLI, skills.sh)

Install these before Stage 1 begins, at Stage 0. Skills are instruction packages that extend the agent's default behavior for a specific domain; installing the right ones up front reduces the chance of the agent improvising its way around the stage gates in Section 12.

Install first, always: the discovery skill itself.

```
npx skills add https://github.com/vercel-labs/skills --skill find-skills

```

If a gap shows up mid-build that nothing below covers (a specific library, an unfamiliar error, a tool not listed here), use this to search rather than improvising from memory.

Install second: the discipline layer. This is not language-specific, it is what makes the stage gates in Section 12 actually get followed instead of skipped under time pressure. This is arguably the single highest-leverage install for this particular project, because the whole risk profile of this build (per Section 16) is an agent convincing itself Raft works when it doesn't.

```
npx skills add https://github.com/obra/superpowers --skill test-driven-development
npx skills add https://github.com/obra/superpowers --skill systematic-debugging
npx skills add https://github.com/obra/superpowers --skill verification-before-completion
npx skills add https://github.com/obra/superpowers --skill using-git-worktrees
npx skills add https://github.com/obra/superpowers --skill finishing-a-development-branch

```

* `test-driven-development` enforces write-test-first, watch-it-fail, then implement, red-green-refactor. This should govern every stage, but matters most for Stage 3 (Raft), where a test that passes without ever having failed first proves nothing.
* `systematic-debugging` is a hypothesis-driven, four-phase root-cause process. Raft election and replication bugs are exactly the kind of intermittent, timing-sensitive bugs this is built for, and exactly the kind that are easy to "fix" by accident without this discipline.
* `verification-before-completion` is the direct enforcement mechanism for every stage gate's checkbox list in Section 12: it requires the agent to actually run the verification commands and read real output before claiming a stage is done, rather than asserting success.
* `using-git-worktrees` and `finishing-a-development-branch` map directly onto the "isolated work, then commit/tag/push" pattern the stage gates require.

Go, gRPC, concurrency, testing, observability, and performance (Stages 1-5):

```
npx skills add samber/cc-skills-golang --skill golang-concurrency
npx skills add samber/cc-skills-golang --skill golang-testing
npx skills add samber/cc-skills-golang --skill golang-grpc
npx skills add samber/cc-skills-golang --skill golang-observability
npx skills add samber/cc-skills-golang --skill golang-performance
npx skills add samber/cc-skills-golang --skill golang-error-handling

```

This one repository covers nearly every technical requirement in Sections 6, 9, 10, and 11 in a single install: `golang-concurrency` for the goroutines/channels/mutex work Raft and the limiter both need, `golang-testing` for table-driven tests and the race detector discipline, `golang-grpc` for the proto/interceptor/error-handling patterns the node-to-node transport needs, `golang-observability` for the structured logging and metrics in Section 10, and `golang-performance` for the benchmark methodology in Section 11.

Docker and Compose (Stage 6):

```
npx skills add BjornMelin/dev-skills --skill docker-compose-architecture

```

Covers Dockerfile and Compose patterns, least-privilege container security, and CI pipeline structure, directly relevant to the 3-node Compose cluster.

React and Vite (Stage 7):

```
npx skills add https://github.com/claudiocebpaz/vite-react-best-practices

```

or, if available in the skills.sh React topic, the Vercel React performance/composition skills. Either covers component architecture, re-render/memoization correctness, and bundle hygiene for the dashboard.

Optional, for self-testing the UI without a human clicking through it (Stage 7 validation): A browser-automation skill (search via find-skills for `agent-browser` or `browser-use`) lets the agent drive the dashboard itself, clicking "kill leader" and confirming the visible handoff, rather than only asserting the UI works. This directly strengthens the Stage 7 manual validation checklist in Section 12 by making it agent-verifiable instead of purely manual.
