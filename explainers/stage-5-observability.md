# Stage 5 — making the system explain itself

**Tagged in git as `v0.6-observability`.**

## What got built

Up to now the system worked but couldn't tell you anything about itself. This stage adds the two things real infrastructure has: **a running commentary** (structured logs) and **live measurements** (metrics).

Both matter for a specific reason. When the dashboard shows a leader dying and a replacement taking over, those numbers are where the story comes from. Without them the demo is a status light changing colour; with them it's "the leader stopped answering, an election started, node-3 won it in 1 millisecond, and here's what that cost."

## What it measures

Seven things, all required by the spec:

- **How long elections take** — and whether this node won or lost
- **Replication lag** — from the leader recording something to each follower actually applying it
- **Request speed** — as p50/p95/p99, split by whether the request was allowed or blocked
- **Refusals, split by reason** — a caller genuinely over budget vs. a request that merely arrived at the wrong machine
- **Which role each node holds** right now
- **Whether each peer is answering**
- **How far the agreed-on history has advanced**

That fourth one is a distinction worth making. During a failover, requests briefly land on machines that aren't the leader. If those were counted as "rejected" alongside genuine over-limit refusals, a routine leadership change would look like a wave of abusive traffic. They're counted separately.

The speed readings describe the **last 10 seconds**, not all history. That was a deliberate choice: with a cumulative average, one bad moment permanently poisons the number and the failover demo would show almost no visible effect. With a rolling window you see the spike *and* the recovery, which is the whole point.

## The design decision I'd defend

**The consensus code doesn't know Prometheus exists.**

The metrics library is used in exactly one package. The consensus core reports through a tiny interface it defines itself, and the monitoring package implements it. Wiring Prometheus directly into the consensus code would have been fewer lines — but it would mean the most correctness-critical part of the system depends on a monitoring library, and every test of it drags that library along.

## Three bugs found by actually reading the numbers

The spec says the gate for this stage is that `/metrics` returns *real, changing* values under live load. I ran a real cluster, pushed traffic through it, and read the output rather than assuming. That caught three things:

**Replication lag was reported as 4.2 seconds.** Real lag should be a few milliseconds. The cause: the leader heartbeats every 75ms, and each heartbeat re-reported the *same* already-applied entry — so it was really measuring "how long since this entry was recorded," a number that grows forever. Fixed by timing each entry once per follower. Now it reads **10.5ms**, which is believable.

**Peer health was never populated.** I'd defined the measurement and never called it. It looked fine in tests because nothing asserted it had a value. Now it refreshes whenever metrics are read, and correctly shows a killed node as unhealthy.

**Speed readings showed zero samples** — which turned out to be correct: my requests had aged out of the 10-second window before I read it. Re-testing with a fresh burst gave real numbers (p50 0.17ms, p99 0.26ms). Worth mentioning because "the code is wrong" and "you measured it wrong" look identical at first glance.

The pattern here is the same one that keeps recurring in this project: **a metric that exists is not a metric that works.** All three bugs would have survived any test that only checked the endpoint returned successfully.

## How it was proven to work

**88 tests repo-wide**, 7 of them new for the monitoring layer, clean across 5 full-repo repeats under the race detector.

The metric tests check things that would otherwise silently rot: that only one role reads as current at a time (so a dashboard can't show a node as two things at once), that the two kinds of refusal stay separate, and that old samples genuinely leave the rolling window rather than accumulating forever.

Then verified live: a real 3-node cluster, 200 requests, the leader killed, and every number checked by hand against what should have happened.

## A number worth being careful about

The election metric says an election took **1 millisecond**. That is *not* how long a failover takes.

It measures from the moment a node decides to run for leader to the moment it wins. Before that, the other machines have to *notice* the leader is gone — which by design takes 300–600ms, because reacting instantly to one missed heartbeat would cause elections constantly.

So the honest failover figure is roughly **300–600ms**, dominated by the detection wait, not the 1ms election. Stage 8 measures it end to end. This is exactly the kind of number that's easy to quote misleadingly, so I'd rather be precise about what's being timed.

## If an interviewer asks

**"Why not just log everything and grep it?"**
Logs answer "what happened," metrics answer "how much and how fast." You can't compute a p99 by grepping without reprocessing everything. They're both here because they answer different questions.

**"Why is the monitoring library isolated to one package?"**
Because the consensus code is the part that has to be correct, and I don't want its dependency list growing. It reports through a four-method interface it owns. If I ever swapped Prometheus for something else, the consensus code wouldn't change.

**"Why rolling-window percentiles instead of cumulative?"**
Because cumulative numbers stop moving. After a few minutes of traffic, a failover spike barely shifts the average, so the dashboard would show nothing during the most interesting moment. A rolling window shows the spike and then the recovery — which is both more useful and more honest.

**"How do you know the metrics are right?"**
I don't trust them by default — three of them were wrong when I first looked. I ran a live cluster, generated real traffic, killed the leader, and checked each number against what should have happened. That's how the 4.2-second replication lag got caught. A metric that exists isn't a metric that works.
