---
name: build-journal
description: Accumulated build learnings for quorumgate, so the same mistake is never made twice. READ this before starting any stage, before debugging a failure, and before trusting any "it passed" signal. APPEND to it whenever something costs time - a wrong assumption, an environment or version gotcha, a green check that proved nothing, a stale instruction in PRD.md, or a correction from the user. Triggers - starting a stage, CI failure, flaky or timing-sensitive test, tooling/version surprise, user correction, anything surprising.
---

# Build journal

This file is the project's memory of what has already gone wrong. It exists because
the risk profile of this build (`PRD.md` Section 16) is an agent convincing itself
something works when it doesn't.

## Protocol

**Before starting a stage or debugging a failure:** read the Learnings below and check
whether the situation matches one. If it does, apply the rule instead of rediscovering it.

**After anything costs time:** append a new entry. The bar for writing an entry is
"this could plausibly bite again," not "this was a disaster." Cheap to write, expensive
to relearn.

**Entry format** — keep all three parts, the rule is the point:

```
### <short title>  (<stage where it happened>)
**What happened:** the observable facts.
**Why:** the actual root cause, not the symptom.
**Rule going forward:** the concrete, checkable behavior change.
```

Do not delete entries when they stop being relevant — the history is the value. If an
entry turns out to be wrong, correct it in place and say what changed.

---

## Learnings

### A green CI check is not proof the pinned version was used (Stage 0)

**What happened:** CI was configured with `actions/setup-go@v5` and `go-version: "1.22"`,
matching the PRD's Go 1.22+ pin. The first CI run passed. Reading the actual log showed
`setup-go` installed Go 1.22.12, then `go: downloading go1.26.5` — the build ran on
**1.26.5**, not 1.22. The Go 1.22 floor was never tested. The green checkmark was real;
what it verified was not what it appeared to verify.

**Why:** `go mod init` writes the *local* toolchain version into `go.mod` (here `go 1.26.5`).
Go 1.21+ defaults to `GOTOOLCHAIN=auto`, so when `go.mod` demands a newer toolchain than
the one installed, the toolchain silently downloads and switches to it. The `go-version`
input in CI becomes a floor for *bootstrapping*, not a ceiling for what runs.

**Rule going forward:** `go.mod`'s `go` directive must state the *minimum supported*
version (`go 1.22` per `PRD.md` Section 7), never whatever `go mod init` happened to write.
More generally: when a check is supposed to prove a version/config constraint, read the
log for what actually executed. A passing job is evidence the job passed, not evidence
the constraint held.

### Floating-point math is not identical across CPU architectures (Stage 1)

**What happened:** The limiter's refill is `tokens + Limit*(elapsed/Window)`. Inspecting
the compiler output showed arm64 emitting `FMADDD` (one fused multiply-add, rounds once)
and amd64 emitting `MULSD`+`ADDSD` (rounds twice). Measured over 5M realistic inputs, the
two forms give different answers **21.5% of the time**. Nodes on different CPUs applying
an identical Raft log would compute different balances and silently diverge — breaking
`PRD.md` Section 14's determinism requirement and Section 9's "all nodes identical" test.

**Why:** The Go spec permits an implementation to fuse a multiply followed by an add into
a single operation, and arm64 has the instruction while amd64 does not. Nothing in the
source hints at it; the divergence is invisible above the assembly.

**Rule going forward:** anything that lands in replicated state must be arithmetic that is
bit-identical everywhere. Force the rounding with an explicit `float64(...)` conversion
around any product that feeds an addition, and keep a test pinned to values where the two
forms actually disagree (`TestRefillArithmeticIsNotFused`). More broadly: "same source, same
inputs" does not imply "same result" — the target architecture is part of the input. Check
this for every future float that crosses a node boundary.

### A measurement can be silently broken by the effect it is measuring (Stage 1)

**What happened:** Chasing the issue above, three claims in a row were wrong. First, a code
comment asserted one refill formula avoided a precision bug — a direct test showed both
formulas gave identical results. Second, a rewritten test suggested the *opposite* formula
was worse. Third, a 20-million-sample sweep reported **0%** divergence between fused and
unfused math, which looked like proof there was no problem at all. That 0% was itself the
bug: the "unfused" comparison expression was being fused by the same compiler optimization,
so it compared fused against fused. Pinning the comparison changed 0% to 21.5%.

**Why:** The experiment ran inside the environment whose behavior was in question, so the
thing being tested silently applied to the test's own control case. A clean-looking result
was mistaken for a verified one.

**Rule going forward:** when a result *disproves* a suspected problem, be more suspicious of
the experiment than relieved by the answer — especially a suspiciously round zero. Verify the
control is actually a control. And never write a confident causal claim into a code comment
without having run the thing that demonstrates it; "plausible mechanism" is not evidence.

### A full disk fails as a build error, not a disk error (Stage 2)

**What happened:** `go test -race ./...` failed with `link: mapping output file failed: no
space left on device`, reported as `FAIL ... [build failed]` next to packages that had just
passed. It reads like a code or toolchain problem. The machine had 202MB free of 228GB.

**Why:** `-race` links much larger binaries than a plain build, so it is usually the first
thing to fail when space runs out — long after ordinary builds still succeed. The error
surfaces as a package-level FAIL, which invites debugging the package.

**Rule going forward:** when a build or link step fails on something that just passed, check
`df -h` before reading any code. `go clean -cache` reclaims a few hundred MB safely (it only
forces a rebuild). Never conclude a stage gate failed on correctness grounds without first
ruling out the environment.

### Passing correctness tests proved almost nothing until they were mutation-tested (Stage 3)

**What happened:** All five of `PRD.md` Section 9's correctness tests passed on the first
run, plus two extras. To check they were real, each safety rule in the implementation was
deliberately broken one at a time. **Four of six planted violations were caught by nothing.**
A node could vote twice per term, vote for a candidate missing committed entries, skip the
log-matching check, or commit an earlier term's entry on replica count alone — and the
entire suite stayed green.

**Why:** Every test began from a converged cluster and killed a node *cleanly*. No log ever
genuinely disagreed with another, so the rules that exist solely to resolve disagreement
were never executed. The tests covered the happy path and tidy failures, which is precisely
where Raft has no interesting bugs. Adding scenarios that force real divergence — a
partitioned leader accepting writes it cannot commit, a node rejoining with the highest term
and the emptiest log, a minority of two that can still confer — took it to 6 of 6.

**Rule going forward:** a correctness test is not evidence until it has been seen to fail
for the right reason. When implementation precedes tests, mutation testing is the substitute
for the missing red phase, and it is not optional on anything safety-critical. Also: some
rules cannot be reached reliably through a live cluster because randomised timing decides
whether the scenario ever occurs. Drive those against the handler directly — a deterministic
unit test beats a cluster test that only exercises the rule once in a thousand runs.

### "The cluster has a leader" does not mean the followers know that (Stage 3)

**What happened:** `TestFollowerRejectsDirectWrites` passed hundreds of times, then failed
on roughly the 15th repeat of the full suite: a follower correctly refused a write but
reported an empty leader hint instead of naming the leader.

**Why:** The test's `waitForLeader` helper returns as soon as one node reports itself leader.
A follower does not learn who won by voting — the candidate it backed might still lose — it
finds out when the first heartbeat arrives. The assertion raced that heartbeat. The empty
hint was correct behaviour: a node that has heard from nobody genuinely does not know.

**Rule going forward:** wait on the condition actually being asserted, not a proxy for it.
"A leader exists" and "everyone knows who the leader is" are different states with a real
gap between them. Repeat-running the suite (`-count=25 -race`) is what surfaced this at all;
a single green run would have shipped it.

### `go get` silently raises the go.mod version floor (Stage 3)

**What happened:** Adding gRPC ran `go get`, which reported `upgraded go 1.22 => 1.25.0` in
passing. That is the Stage 0 toolchain trap returning by a different route: CI pins
`go-version: "1.22"`, so a 1.25 floor would have made every CI run silently download a newer
toolchain again, and the pinned floor would have gone back to meaning nothing. Resetting the
directive by hand did not hold - `go mod tidy` kept restoring 1.25.

**Why:** gRPC itself only needs Go 1.22.7. The floor was being dragged up by a *transitive*
dependency, `golang.org/x/net`, whose current release requires 1.25. `go get` resolves to
newest-compatible and then raises the directive to match whatever that pulled in, so the
constraint arrives from a package never named in the command.

**Rule going forward:** after any dependency change, re-read the `go` directive before doing
anything else. When it moved, find the dependency that demands it with
`go list -m -f '{{.Path}} {{.GoVersion}}' all` rather than guessing, then pin that specific
module to a contemporaneous release. And verify the floor for real:
`GOTOOLCHAIN=go1.22.12 go test ./...` runs the build on the exact toolchain CI installs and
proves no upgrade is triggered, which reading go.mod alone cannot.

### The untested code is always the code that only runs when things go wrong (Stage 4)

**What happened:** Mutation testing on the replicated limiter caught 3 of 5 planted
violations. The two that survived were the two rules that prevent double-counting: accepting
a superseded proposal as success, and retrying a request whose outcome is unknown. Every
other test passed with both rules deleted. This is the second stage running where mutation
testing found exactly this shape of gap - Stage 3 caught only 2 of 6 for the same underlying
reason.

**Why:** Both rules execute only when leadership changes *during a single request*. The
cluster tests killed the leader between requests, which is easy to arrange and exercises
none of that code. Naturally-written tests cover the tidy path; the dangerous code lives in
the untidy one, and timing decides whether a live cluster ever reaches it.

**Rule going forward:** when a rule only fires in a narrow race, do not rely on an
integration test stumbling into it. Put the dependency behind a small interface and drive the
scenario directly - here, a fake node that scripts a proposal's fate. Ask of every safety
rule: "what has to go wrong for this line to run, and does any test make that happen?" If the
answer is timing, write the unit test.

### A retry is only safe when you can prove the first attempt did nothing (Stage 4)

**What happened:** Building automatic retry for a smooth failover, the obvious design -
retry whenever a request fails - would silently double-count. A timeout does not mean the
command failed; it means the outcome is unknown, and the command may well have committed.

**Why:** Retrying is safe only with proof of non-execution. Three failures carry that proof:
the node reported it was not the leader (never appended), the connection was refused (never
delivered), or the entry at that log position now holds a different term (definitively
discarded). A timeout carries no proof at all.

**Rule going forward:** classify every failure as "provably did not happen" or "unknown"
before deciding to retry, and never retry the second kind without idempotency keys. For a
counter this matters more than usual: a lost request is visible to the caller, while a
double-count is silent and permanent.

### A metric that exists is not a metric that works (Stage 5)

**What happened:** Three of seven metrics were wrong the first time a live cluster was
inspected. Replication lag read **4.2 seconds** where the true value is ~10ms; peer health
was defined but never populated by any call site; and latency percentiles reported zero
samples. Every unit test passed, and the `/metrics` endpoint returned 200 throughout.

**Why:** The lag bug is the instructive one. The leader heartbeats every 75ms, and each
heartbeat re-reported the same already-applied index, so the measurement was really "time
since this entry committed" - a quantity that grows without bound while the cluster is idle.
Nothing in a unit test would notice, because the bug only appears once heartbeats repeat
over an index that has stopped advancing. The zero-samples case was not a bug at all: the
requests had aged out of the rolling window before the read. "The code is wrong" and "you
measured it wrong" look identical until you check.

**Rule going forward:** an endpoint returning 200 is not evidence the numbers in it are
right. Read every metric under real load and ask whether the magnitude is physically
plausible before believing it - 4.2 seconds for a loopback round trip should be obviously
wrong on sight. Check that every collector defined actually has a call site; an unpopulated
gauge silently reads as zero, which looks like data. And confirm a metric that measures a
per-event quantity is recorded once per event, not once per report.

### Two individually reasonable settings were wrong in combination (Stage 8)

**What happened:** Under a sustained 3,000 req/s load the cluster ran cleanly for 27 seconds
and then collapsed into an election storm - terms climbing 3, 4, 5, 6, 7, 8, 9 with nodes
thrashing between candidate and leader until every request failed. Separately, a leader kill
that should have been nearly invisible cost 415 requests.

**Why:** Two configuration values, each defensible alone. The election timeout (300-600ms)
was tuned for a visibly fast demo failover, but under load the leader's heartbeats queued
behind real request work and arrived late, so followers declared a healthy leader dead and
the winner was immediately starved the same way. And the API's failover grace (1s) was
shorter than the election timeout it had to outlast, so in-flight requests gave up just
before the new leader appeared. Raising the election timeout to 1-2s and the grace to 3s
gave 100% success at 10,000 req/s with a leader killed mid-run.

**Rule going forward:** timeouts that interact must be checked as a set, not individually.
The ordering that has to hold here is heartbeat << election timeout < failover grace. Also:
settings tuned for a demo are not settings tuned for load, and the difference only appears
under *sustained* traffic - a short burst test would have shown neither problem, since the
first took 27 seconds to develop.

### A benchmark that shares a machine with its target measures the harness (Stage 8)

**What happened:** Cluster throughput probes returned nonsense above ~10k req/s: 12k/s gave
30% success, 16k/s gave 4.6%, and 20k/s gave 99.9% - a *higher* rate producing a far better
result than a lower one. Some failures carried HTTP status 0 with 30-second latencies.

**Why:** vegeta ran on the same 8-core laptop as all three server processes. At high rates
the load generator starves the servers it is measuring, and status 0 with 30s latencies is
client-side connection exhaustion rather than any server behaviour. The non-monotonic
results are the signature: a real capacity limit degrades monotonically.

**Rule going forward:** when throughput does not degrade monotonically with offered load,
suspect the harness before the system. Report the highest rate that reproduces cleanly and
state the co-location as a limitation, rather than quoting the best number ever observed.
Distinguish client-side errors (status 0, connection refused) from server-side ones (503) -
they mean opposite things about what was actually measured.

### Freezing a node's network is not enough; its clock has to stop too (Stage 6)

**What happened:** The dashboard needs a "kill this node" button that is undoable. The
obvious build - block the node's traffic in both directions - was wrong in a way that only
shows up after the node comes back. A node whose network is severed still has a running
election timer, so it campaigns against peers it cannot reach and raises its term on every
attempt. Measured: a follower "down" for **2 seconds came back at term 10 while the cluster
was still at term 1**, and rejoining forced a pointless election among three healthy nodes.
Passing the injector's own clock to `raft.Config.Clock` and stopping it while frozen fixed
it: the node resumes exactly where it left off, hears the current leader, and steps down.

**Why:** Raft decides to hold an election because its clock says too much time has passed.
Cutting the network removes the node's ability to *win* an election but not its motivation
to *start* one. A suspended machine has no clock advancing, so modelling the fault at the
clock is both simpler and closer to what a stopped machine actually is.

**Rule going forward:** when simulating a failure, ask what the failing component actually
stops doing, not just what stops reaching it. And prefer the seam the design already offers
- `Clock` was an injectable interface from Stage 3, so the entire fault system was built
without editing one line of consensus code, which is what kept the Stage 3 correctness
tests meaningful.

### A frozen node goes on advertising the role it held (Stage 6)

**What happened:** After killing the leader through the new control, `/status` on that node
still reported `role: "leader"`. Every unit test passed. It surfaced only in a live
three-node run, where a script looking for "the leader" found the dead one first and
concluded failover had not happened. A dashboard would have drawn two leaders at once.

**Why:** Freezing a node preserves its state, and its state said "leader". That view is
stale by definition - the node cannot know it has been replaced, because finding out
requires exactly the traffic that was cut off. The bug was not in the freeze; it was in
reporting an internal state as though it were a fact about the world. `PRD.md` Section 8
already had the answer: a node is drawn as leader, follower, **or down**.

**Rule going forward:** when a component is deliberately isolated, its self-report describes
the past, not the present. Anything aggregating those reports needs a state for "this node
cannot currently know" rather than trusting the last thing it said. Check every status field
a fault can invalidate, not just the one the fault was aimed at.

### A validation script proved three nodes agreed by comparing three empty strings (Stage 6)

**What happened:** The Stage 6 validation reported **18 of 18 checks passing**. Two of them
were worthless. The replication check grepped `/metrics` for `quorumgate_raft_commit_index`,
but the metric is named `quorum_raft_commit_index` - so it captured empty output from all
three nodes, compared `"" = "" = ""`, and reported that every node agreed on the commit
index. The same bug passed the "restarted node was backfilled" check. Fixing the name and
re-running gave the real evidence: 2476 -> 2501 across 25 requests, identical on all three.
A second, separate instance of the same class: anchoring the grep with a trailing space
(`'^quorum_raft_commit_index '`) also matched nothing, because the metric carries a
`node_id` label and is followed by `{`.

**Why:** Comparing values for equality treats "both absent" as "both agree". Every step of
the pipeline - curl, grep, awk - succeeds on no matches, so nothing anywhere reports a
problem. This is the Stage 1 lesson (a suspiciously clean result is a reason to distrust the
experiment) arriving through a different door, and the Stage 5 lesson (`/metrics` returning
200 says nothing about the numbers in it) applied to the checker rather than the metric.

**Rule going forward:** any check comparing extracted values must first assert the value was
extracted - treat empty as failure explicitly, never as a match. Print the values a check
compared, not just its verdict; the empty output was visible in the log the whole time and
was only noticed by reading it. And when grepping metrics, match on `[ {]` after the name,
since a labelled metric is not followed by a space.

### PRD.md Section 18 names a skill that does not exist (Stage 0)

**What happened:** `npx skills add BjornMelin/dev-skills --skill docker-compose-architecture`
failed with "No matching skills found." The repo has 89 skills; the closest real match is
`docker-architect`, which covers the same ground (Dockerfile/Compose patterns, hardening,
CI). Installed that instead and documented the substitution in `CLAUDE.md`.

**Why:** `PRD.md` was written ahead of the build and its external references can drift —
upstream repos rename or remove skills. The PRD is the source of truth for *intent and
scope*, but it cannot be authoritative about the current state of third-party repos.

**Rule going forward:** when a PRD instruction names an external artifact (skill, package,
tool, version) and reality disagrees, do not silently improvise and do not treat the PRD as
wrong about intent. Find the closest real equivalent, use it, and record the substitution in
`CLAUDE.md` plus an entry here. Surface the discrepancy to the user rather than burying it.
Scope decisions still come from the PRD; only the external identifier changed.
