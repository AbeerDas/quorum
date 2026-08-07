# Stage 8 — measuring it for real

**Tagged in git as `v0.9-benchmarked`.** Every number here was measured. None were estimated.

## Why this stage exists at all

It's easy to say a system is fast. The spec for this project is unusually strict about it: figures must be **real measured numbers or the section doesn't get written**. That rule exists because a made-up performance number is the easiest thing in the world to write and the easiest thing in the world for an interviewer to destroy — the follow-up is always "how did you measure that?"

So this stage is a load-testing tool, three scenarios, and a script anyone can re-run.

## The headline numbers

**One machine, no replication: 41,362 requests/second.**

**Three machines, every request agreed by a majority first: 10,000 requests/second** sustained for 30 seconds, 100% success, 99% of requests answered in under 25ms.

**Killing the leader mid-benchmark cost 10 requests out of 90,000** — 0.011% — with **zero seconds** where the cluster served nothing.

Measured on an Apple M2 laptop with the load generator running on the same machine as the servers, so the servers were competing with the thing hammering them. That makes these a floor, not a ceiling.

## What replication actually costs

At the same load, comparing one machine to three:

| | p50 | p99 |
|---|---|---|
| Single machine | 0.09 ms | 2.99 ms |
| 3-machine cluster | 0.18 ms | 5.94 ms |

**Roughly double.** That's the honest price of the guarantee: a request isn't answered until a majority of machines has agreed to record it, so the answer survives the machine that gave it. Doubling a sub-millisecond number to buy that is a good trade, and being able to state the cost precisely is more useful than claiming there isn't one.

## The result I'd actually lead with

Killing the leader halfway through a 30-second load test, with traffic deliberately aimed at a *different* machine so the redirect path was exercised too:

- **89,990 of 90,000 requests succeeded** (100.0%, 10 failures)
- **Zero seconds** with the cluster fully down
- Both surviving machines finished holding **exactly the same count**

That last point is the one worth explaining, because it's how "zero lost or double-counted" gets *proven* rather than claimed.

Every accepted request becomes exactly one entry in the shared history. So the length of that history is an **independent audit** of how many requests were accepted — one that doesn't rely on the counter being right. Both survivors ended at position **89,990**, and exactly **89,990** requests were told "yes."

Not approximately. Exactly, and both machines agreed.

If a request had been lost, the history would be shorter than the count. If one had been counted twice, it would be longer. It was neither.

## Two things I got wrong, and what they taught me

**My default settings collapse the cluster under sustained load.**

The system waits 300–600ms without hearing from the leader before assuming it's dead. I picked that so a demo failover looks satisfyingly quick. Under 3,000 requests/second, it turns out the leader gets so busy answering requests that its "I'm still here" heartbeats arrive late — so the others declare it dead *while it's perfectly healthy*, hold an election, and the winner immediately gets starved the same way. The cluster spent 27 seconds fine and then fell over into a spiral of elections.

Raising the wait to 1–2 seconds (roughly what real systems like etcd use) made it stable indefinitely at 10,000/s. The tradeoff is real and worth stating plainly: **a snappy demo and stability under load want opposite settings.**

**A second setting had to be raised to match.** When a request arrives during a failover, the receiving machine holds onto it and retries against the new leader rather than failing immediately. But it was only willing to wait 1 second — less than the 1–2 seconds detection now takes — so requests were giving up just before help arrived. Once that patience exceeded the detection time, failures dropped from 415 to 10.

Neither bug was in the consensus logic. Both were **settings that were individually reasonable and wrong in combination**, and neither would have surfaced without running a real sustained load test.

## Honest limitations

The load generator ran on the same laptop as all three servers, competing for the same 8 cores. At rates above ~10,000/s the measurements became erratic — one run reporting 30% success and the next 99% at a *higher* rate — which is the benchmark harness struggling, not the system. I stopped at rates I could reproduce rather than quoting the best number I ever saw.

This also means **the real ceiling is higher than 10,000/s.** I can't say how much higher without separate machines, so I don't.

## If an interviewer asks

**"10,000 requests a second — is that good?"**
For a system doing a majority agreement on every single request, on a laptop that's also generating the load, yes. The more meaningful number is the comparison: replication roughly doubles latency versus a single machine. That's the cost of the guarantee, and I measured it rather than guessing.

**"How do you know nothing was lost or double-counted?"**
Because I didn't ask the counter — I audited it against something independent. Every accepted request becomes exactly one entry in the shared history, so the history's length should equal the number of requests answered "yes." It was 89,990 on both surviving machines against 89,990 successes. A loss would make it shorter, a double-count longer.

**"What broke during benchmarking?"**
My own defaults. Aggressive election timeouts made the leader look dead under load because its heartbeats were getting queued behind real work, and the cluster fell into an election spiral. And the retry window was shorter than the detection time, so requests gave up just before the new leader appeared. Both were configuration, not logic — and both only appeared under sustained real load.

**"Why not just run it on bigger machines for better numbers?"**
I'd rather publish a number I can reproduce on hardware I describe than a bigger one that's hard to trust. The script is in the repo; anyone can run it and get their own.
