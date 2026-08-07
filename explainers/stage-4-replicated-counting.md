# Stage 4 — putting the counter behind consensus

**Tagged in git as `v0.5-replicated-state`.** This is the stage where the project became a real distributed system rather than two good pieces sitting next to each other.

## What got built

Before this stage there was a counting engine ([Stage 1](stage-1-counting-engine.md)), a web server ([Stage 2](stage-2-the-front-door.md)), and a consensus engine ([Stage 3](stage-3-consensus.md)) — but the counter and the consensus engine had never been introduced to each other. There was no working three-machine rate limiter.

Now there is. **Every rate-limit decision goes through the cluster before it is answered.** A request doesn't get "yes" until a majority of machines has agreed to record it. That's slower than deciding on the spot — each request costs a round trip — and it's the entire point: the answer survives the machine that gave it.

## The problem that shaped the design

Here's a subtle thing that would have quietly broken everything.

The counter refills over time — "you've earned 2 more requests since we last spoke." So **the current time is an ingredient in the answer**, not just background. If each machine looked at its own clock while recording a request, three machines would compute three slightly different balances from the identical instruction, and they'd drift apart forever.

The fix is that **the leader stamps the time onto the instruction itself**, and every machine replays it with that same stamped time. The machines aren't running a clock; they're replaying a recipe that already has the timestamp written into it.

This is why [Stage 1](stage-1-counting-engine.md) made time an explicit input rather than something the counter reads internally — a decision made three stages earlier specifically so this would work. It's the single highest-leverage design choice in the project.

There's a smaller cousin of the same bug: a timestamp in Go remembers its timezone. Two machines in different regions describing the *same instant* would compare as different. So timestamps are normalised before use. There's a test for that one specifically.

## The rule that prevents double-counting

This is the part I'd want to be asked about.

When a request fails, the obvious instinct is to retry it. **For a counter, that instinct is dangerous.** If the request actually succeeded and you just didn't hear back, retrying spends a second token for one request. The customer gets charged twice.

So the system sorts failures into two categories:

**"Definitely didn't happen"** — the machine said "I'm not in charge, ask someone else," or the connection was refused outright, or the instruction was provably thrown away when leadership changed. In all three cases there is *proof* nothing was recorded. Safe to retry, and it retries automatically, which is what makes a failover look seamless instead of producing a burst of errors.

**"Don't know"** — a timeout. The request may have been recorded, or may not. **This is never retried.** It returns an error and lets the caller decide. Losing a request is bad; silently counting it twice is worse, and unlike a lost request, nobody would ever notice.

## The other thing: you can talk to any machine

Only one machine at a time is allowed to accept writes. But requiring callers to know which one would be miserable. So if a request lands on the wrong machine, that machine **quietly passes it to the leader and returns the answer itself**. Callers never know or care.

Requests are marked when passed along, so a request can't circle the cluster forever if leadership keeps moving — after one hop it fails honestly instead.

## How it was proven to work

**81 tests across the project now**, 17 of them new for this stage, plus 6 new ones covering the request-routing behavior.

Two of them are the ones that matter:

**"Killing the leader mid-count neither loses nor double-counts."** Sixty requests are fired at the cluster, the leader is killed halfway through, and then the arithmetic has to be exact: the tokens actually spent must equal the number of requests the client was *told* were allowed. Not approximately — exactly, on every surviving machine, and all machines must agree with each other.

**Mutation testing again, and it caught the same class of gap as Stage 3.** I deliberately broke five safety rules to check something would notice. The first pass caught only three. The two that slipped through were — of course — **the exact two rules that prevent double-counting**, because they only run when leadership changes in the middle of a single request, and none of my tests ever created that. I made those rules directly drivable in tests and got to five of five.

That's twice now that the same lesson has landed: *the tests I wrote naturally covered the tidy cases, and the dangerous code only runs in the untidy ones.*

**And it was verified for real, not just in tests.** Three actual server processes, real network connections: three requests sent deliberately to a *follower* were forwarded and answered (remaining 4, 3, 2), then the actual leader process was killed, a new leader was elected, and the next requests continued at **1, then 0** — rather than resetting to 4. The count survived the death of the machine that recorded it. Then the sixth request was correctly refused.

## Two honest notes

**A leadership change happened once in a healthy cluster** while I was testing, with no failure to cause it. I chased it: running an idle cluster for 45 seconds showed exactly one election, so it isn't systematic. The likely cause is that my laptop was simultaneously running heavy test suites, and a several-hundred-millisecond stall is enough to trigger an election when the timeout is set that low. The cluster recovered correctly, which is the actual point — but I'd rather record it than pretend it didn't happen.

**The race detector caught a bug in my own test code** during the final check — two goroutines touching the same variable. Production code was clean. Worth mentioning because it's the reason that check is mandatory rather than optional.

## If an interviewer asks

**"Why does every request need consensus? Isn't that slow?"**
It is slower — that's the trade. The alternative is deciding locally and replicating afterwards, which is faster but means a machine dying takes the last few decisions with it. Since the entire premise is that the count survives failure, paying a round trip per request is the honest price. The benchmark stage will measure exactly what that costs.

**"What happens if a request fails halfway through?"**
Depends on whether we can prove it didn't happen. If a machine says "not me" or refuses the connection, nothing was recorded and we retry safely. If we simply time out, we genuinely don't know — so we return an error rather than retry, because retrying an unknown is how you count one request twice. A lost request is visible; a double-count isn't.

**"How do you know all three machines have the same numbers?"**
Because the decision isn't made per-machine. The leader writes down an instruction with the time already stamped into it, and every machine replays the identical instruction. There's a test that runs the same sequence on two independent copies and requires the results to be byte-identical, and another that proves a machine applying the log much later still lands in exactly the same state.

**"What's still missing?"**
Metrics and structured logging, a Docker setup so it runs as three containers with one command, the dashboard, and real benchmark numbers. The distributed system itself is now complete and demonstrable from a terminal.
