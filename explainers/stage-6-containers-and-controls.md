# Stage 6 — one command, three machines, and a kill switch

Up to now, running this system meant opening three terminal windows and typing three long
commands with matching port numbers. That works, but it makes the thing sound like a science
project. This stage turns it into one command:

```bash
docker compose up --build
```

Three nodes come up, find each other, elect a leader, and start replicating. It takes about
ten seconds.

The other half of this stage is less obvious but more interesting: the system can now be
**broken on purpose, from a web request**. That matters because the whole claim of this
project is "it survives failure." A claim like that is only worth anything if someone can
make the failure happen while watching.

---

## Part one: the containers

A container is a way to package a program with everything it needs to run, so it behaves the
same on any machine. Three containers, one per node.

Two decisions worth explaining.

**The image is tiny and nearly empty — 22 MB.** The final image contains the compiled
program and essentially nothing else: no shell, no package manager, no operating system
utilities. This is a security posture more than a size optimisation. Software you don't ship
can't be exploited, and the usual way an attacker escalates inside a container is by using
the tools that happen to be lying around in it. There aren't any here.

That creates a small puzzle. Docker likes to periodically check "is this container still
healthy?", and the normal way to answer is to run a tool like `curl` inside it. There's no
`curl`. The fix is that the node's own program answers the question:

```
quorumd -healthcheck
```

Run that way, it makes one request to itself, exits successfully if the node is serving, and
exits with an error if it isn't. The health check is the same binary, invoked a second way,
rather than a whole extra tool the image would have to carry and keep patched.

**The build pins the Go version and means it.** There's a recurring trap in this project,
recorded twice already in the build journal: Go will quietly download a *newer* version of
itself if a project's configuration asks for one. That means you can pin a version, watch the
build pass, and have proved nothing — the pinned version was never the one that ran.

The container build sets a setting (`GOTOOLCHAIN=local`) that forbids this substitution
entirely. The image starts from Go 1.22, and the build cannot silently switch to anything
else. So when the image builds successfully, that is real evidence that the project genuinely
works on the version it claims to support. A build step became a test.

---

## Part two: the kill switch

The dashboard (Stage 7, next) needs to let someone break the cluster without touching a
terminal. That means the server has to offer the failures as buttons. Four were built:

| Control | What it simulates |
|---|---|
| **Kill** | The machine crashed. Gone instantly. |
| **Pause** | The machine hung. Still there, answering nothing. |
| **Delay** | The network got slow, but nothing is broken. |
| **Revive** | Undo. Put it back to normal. |

**Kill and pause are genuinely different, and the difference is the interesting part.**

When a machine crashes, its peers find out immediately — like calling a disconnected number
and getting an error tone straight away. When a machine *hangs*, it's like calling a number
that rings forever: nobody picks up, but nobody tells you that either. You have to decide for
yourself how long to wait before giving up.

The second one is worse for the cluster, and that is counterintuitive enough to be worth
showing. A crashed node is detected fast. A hung node makes everyone else sit and wait out
their own timeouts. In distributed systems this is a well-known unpleasant truth: the
half-dead machine causes more trouble than the dead one.

**Delay is the "why does timing matter" control.** Nothing is broken; messages just take
longer. A small delay changes nothing at all. Increase it far enough and nodes start
concluding a perfectly healthy leader has died, and the cluster churns through elections it
didn't need. That's a live demonstration of why the timing settings in a consensus system are
load-bearing rather than arbitrary.

---

## The mistake I made, and what fixed it

The obvious way to build "kill this node" is to cut its network connections. Block what it
sends, block what it receives, done.

I built that. It's wrong, and the reason is genuinely surprising.

A node that can't reach anyone doesn't sit quietly. Its internal timer keeps running, it
notices it hasn't heard from the leader, and it decides to hold an election. It loses, because
nobody can hear it. So it waits and tries again. And again. Each attempt increments a counter
called the **term**, which is essentially "which round of leadership are we in." Terms only
ever go up, and a higher number always wins.

So an isolated node sits alone in the dark, holding elections against nobody, its term
climbing the whole time. Then you revive it — and it walks back in with a term far ahead of
everyone else's, and the entire healthy cluster has to stop and hold a new election just to
absorb it.

The measurement, from the test that catches this: a node away for **2 seconds came back at
term 10, while the cluster was still at term 1.** Two seconds. A minute-long demo would come
back in the hundreds.

The fix came from asking a better question. Instead of "how do I cut this node off," ask
"what does a stopped machine actually stop doing?" A suspended machine doesn't experience
time passing. So the fault doesn't block the node's network — it **stops the node's clock**.

With no clock, the node never notices time passing, never decides to hold an election, and
never touches its term. When revived, it picks up exactly where it left off, hears from the
current leader within a heartbeat, sees a higher term, and quietly steps down to follower.

**One honest caveat, which the continuous integration server found and my laptop didn't.**
Stopping the clock prevents a node from *starting* an election. It can't un-start one that
began a fraction of a second earlier. So if a node is killed at the exact instant its timer
fired, it freezes one term ahead, and on the way back it can cost the cluster one election.

I originally wrote the test to demand the term be *identical* after a revival. It passed ten
times locally and then failed in CI, which hit that narrow window. The assertion was wrong,
not the code — a real machine can crash mid-election too.

So the guarantee is a **bound, not a zero**: rejoining costs at most one election. What the
frozen clock rules out is the version where the damage grows with the length of the outage —
a term per timeout, forever. Two seconds away was already the difference between term 2 and
term 10. A node away for a minute would be hopeless; with the clock stopped, a node away for
a minute costs exactly as little as one away for a second.

There's a detail I'm quietly pleased with: **none of this required changing the consensus
code.** Back in Stage 3, the Raft implementation was written to take its clock as a
replaceable input, so that timing could be controlled in tests. That decision, made for
testing reasons months of work earlier, is what allowed the entire fault system to be built
without touching the most correctness-critical code in the project. The Stage 3 tests still
test exactly what they tested before.

---

## The second mistake, which only a live run could catch

Every unit test passed. Then I ran three real nodes, killed the leader, and my script
reported that the dead node was still the leader.

It wasn't a bug in the killing. It was a bug in the *reporting*. Freezing a node preserves
its state — and its state said "I am the leader." It has no way to learn otherwise, because
finding out would require exactly the messages that were cut off.

So the node was honestly reporting a fact that had stopped being true. A dashboard would have
drawn two leaders at once, and anyone counting them would have concluded the consensus
protocol had broken.

The spec already had the answer. It describes each node as being drawn as "leader, follower,
**or down**." A node that's been switched off doesn't report the role it's remembering — it
reports that it's down. One line of code; the lesson is that I'd been treating a node's
opinion about itself as a fact about the cluster.

---

## The traffic generator

One more piece: the dashboard needs to be able to *create* load, so the limiter can be seen
allowing and then blocking in real time. Making the viewer install a load-testing tool first
would defeat the point.

So the node generates its own. You give it a rate and a duration, and it fires requests at
itself through its own front door — the same path a real client takes, including being
forwarded to the leader and replicated across the cluster.

It offers two traffic patterns, and the second one makes the project's core point:

- **many_fair** — twenty polite callers sharing the traffic. Nobody gets blocked.
- **one_abusive** — one caller sending 90% of the requests, plus a few well-behaved ones.

From a real run against the containerized cluster:

```
abusive caller:  767 allowed, 404 blocked
well-behaved:    118 allowed,   0 blocked
```

The greedy caller burns through its budget and gets cut off. The polite callers, sharing the
same three servers at the same moment, notice nothing at all. **Zero** of their requests were
blocked. That's the entire promise of a per-caller rate limiter, visible in two lines.

One honest detail in that output: the generator separately counts requests it *couldn't send*
because it fell behind its own target rate. Those are reported as "dropped," not "failed."
Conflating them would blame the cluster for the load generator running out of room — which is
exactly the mistake Stage 8 caught in the benchmarks.

---

## How this was proven

There's a script in the repo, `scripts/validate-cluster.sh`, that runs 18 checks against a
live containerized cluster. All 18 pass. Highlights:

- The cluster forms from one command and exactly one node claims leadership.
- **25 requests produced exactly 25 new log entries**, and all three nodes reported the
  identical commit index (2476 → 2501). The log's length is an independent audit of the
  request count: losing a request makes it shorter, double-counting makes it longer.
- Killing the leader elected a new one, and **30 out of 30** requests were served during the
  handoff.
- The dead node's term stayed put while it was down, and on this run it rejoined as a
  follower without unseating the new leader. (Usually, though not guaranteed — see the
  caveat above. What *is* guaranteed, and tested, is that rejoining costs at most one
  election no matter how long the node was away.)
- A *real* container crash (`docker compose stop`) was survived too, and the restarted node —
  which comes back with no memory at all, since nothing is written to disk — was refilled
  from the leader's log until it matched exactly.

### A near-miss worth admitting

The first version of that script reported **18 of 18 passing**, and two of those were
worthless.

The replication check looked for a metric called `quorumgate_raft_commit_index`. The metric is
actually called `quorum_raft_commit_index`. So the check found nothing on all three nodes,
compared three empty results, found them identical, and cheerfully reported that all three
nodes agreed on the commit index.

Nothing errored. Every command in the chain succeeds when it finds nothing. The failure was
visible in the output the whole time — the line printed `commit index per node:` with nothing
after it — and I only caught it by actually reading the numbers instead of the verdicts.

The fix is in the script now: an empty value is treated as a failure rather than a match, and
every check prints the values it compared, not just pass or fail. The real numbers came out
of the corrected run, and they're the ones quoted above.

---

## What this is not

Being straight about the limits:

- **Nothing is written to disk.** A restarted node comes back with no memory and is refilled
  from the leader. That's fine with a majority alive, and it's a deliberate scope decision,
  but a real deployment would persist to disk.
- **The demo controls are dangerous by design.** They let anyone who can reach the node stop
  it, with no authentication. They're switched off unless you explicitly pass
  `-demo-controls`, and the node logs a warning when they're on.
- **There's still no dashboard.** That's Stage 7. Everything described here is currently
  driven with `curl`.

---

## If an interviewer asks

**"Why not just use `docker stop` for the kill button?"**
Because the dashboard would need access to Docker's control socket to do it, and handing a
web application the ability to start and stop containers is a genuinely bad idea in a repo
people are going to read. It also wouldn't be undoable from the UI. The in-process freeze is
indistinguishable from a crash as far as the other nodes are concerned — they see a peer that
stopped answering — and it can be undone with a button. I did test real container crashes too;
that's check 8 in the validation script.

**"Isn't stopping the clock cheating? A real crashed machine doesn't 'resume'."**
Partly, yes, and it's worth being precise about what's being modelled. To the rest of the
cluster the simulation is exact: a peer that stops answering, then reappears. What's not
exact is the revived node's own memory — a truly crashed process would come back empty, since
nothing is persisted. So the freeze models a *suspended* machine rather than a crashed one.
The crashed case is covered separately by stopping the container for real, and the node
handles it: it comes back empty and the leader refills its log.

**"Why is a hung node worse than a dead one?"**
A dead machine refuses connections immediately, so its peers find out fast and get on with
electing a replacement. A hung machine accepts the connection and then never answers, so
everyone waits out their full timeout before concluding anything. The cluster survives both,
but the hung one costs more time and holds up the leader's work while it waits. That's why
both controls exist rather than one — they look similar and behave differently.

**"How do you know the replication actually worked, rather than the counter just looking
right?"**
The commit index is the number of entries in the replicated log, which is maintained by the
consensus layer and not by the request handler. Twenty-five requests produced exactly
twenty-five new entries on all three nodes. If a request had been lost the log would be
shorter; if one had been counted twice it would be longer. It's an independent check, because
the thing being audited isn't the thing keeping the count.

**"Your test passed locally ten times and then failed in CI. What did you change?"**
The assertion, not the code. I'd claimed a revived node comes back at exactly the term it
left at. CI hit the narrow window where a node is frozen a fraction of a second after its
election timer fired, so it froze one term ahead — which is legitimate, since a real machine
can crash mid-election too. The fix was to assert what the design actually guarantees: the
cost of rejoining is bounded at one election and doesn't grow with the outage. I also made
the test wait out any in-flight election before reading the term, so it measures the frozen
state rather than racing it. The tempting alternative was to add a retry until it passed,
which would have buried a real fact about the system in a flaky test.

**"Your validation said 18 out of 18. How much should I trust that?"**
More than I would have trusted the first version, which also said 18 out of 18 with two
checks that compared empty strings to each other. The script now treats a missing value as a
failure and prints every number it compared, so the output can be read rather than just the
verdict. That's the second time in this project a green check turned out to prove nothing —
the first was in Stage 0, where a passing CI run was verifying a Go version it wasn't
actually using. It's the failure mode I now look for first.
