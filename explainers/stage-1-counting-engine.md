# Stage 1 — the counting engine

**Tagged in git as `v0.2-limiter-core`.**

## What got built

The part that actually decides "yes, you may" or "no, slow down" — and nothing else. No web server, no network, no multiple machines. Just the counting logic, on its own.

That isolation is deliberate. It's the same reason you test an engine on a stand before bolting it into a car: when something goes wrong later, you want to already know the engine isn't the problem.

## How the counting works

Each caller gets a **bucket of tokens** — say 10.

- Every request spends one token.
- Run out, and you're blocked.
- Tokens **refill gradually over time**, like a phone trickle-charging.

Two nice properties fall out of this design for free:

**You're never stuck.** Blocked callers recover on their own as tokens trickle back. There's no reset moment to wait for and no human intervention.

**Bursts are allowed.** Idle for a while? Your bucket is full, so you can fire off all 10 at once. That matches how real clients actually behave — quiet, then a flurry — instead of punishing them for it.

The technical name is a **token bucket**, and it's what most production rate limiters use.

## How it was proven to work

The code was written **test-first**: describe what "correct" looks like, watch that test fail because nothing exists yet, then write just enough code to make it pass.

That ordering matters more than it sounds. A test written *after* the code usually passes on the first run — which proves nothing, because you never saw it capable of failing. Watching it fail first is the only way to know the test is actually wired up to the thing it claims to check.

**16 tests** now cover this. The ones worth mentioning:

**One noisy caller never affects anyone else.** This is the core fairness promise of the entire project. The test hammers one caller until they're blocked, then confirms a second, unrelated caller sails straight through, untouched.

**Under real simultaneous load, the count is exact.** 500 requests fired at once against a limit of 100 let through **exactly 100** — not 99, not 101. This runs under Go's *race detector*, a tool that watches for two operations stepping on each other at the same instant. This is the category of bug that never appears in a demo and then shows up randomly in production at 2am.

**Replaying the same requests produces the identical result.** Two independent copies fed the same sequence end up in exactly the same state. That's not interesting yet — but it's the foundation the three-machine version depends on completely, so it's proven now rather than assumed later.

**Weird conditions are handled.** The clock jumping backwards doesn't hand out free tokens. Changing the limit mid-flight takes effect immediately. Callers who've gone quiet get cleaned out of memory — but only when doing so provably can't hand anyone a refund they didn't earn.

## The thing I'd actually bring up

While testing this stage I found a bug that had nothing to do with the counting logic and everything to do with what the project promises: **[the chip-rounding bug](stage-1-bug-chip-rounding.md)**.

It's the better story of the two. Read that one.

## If an interviewer asks

**"Why write the tests first?"**
A test written afterward passes immediately, and you learn nothing from that. I never saw it fail, so I have no evidence it can catch the bug it's supposedly guarding. Writing it first forces that failure and proves the test works.

**"Why test 500 simultaneous requests?"**
Because "exactly 100 got through" is the whole promise, and concurrency is where that promise quietly breaks. Two requests arriving at the same instant can both read "1 token left" and both spend it. That bug is invisible in normal use and impossible to reproduce on demand — so it gets tested explicitly rather than hoped about.

**"What's left to do?"**
This stage is one machine counting correctly. The hard part — three machines agreeing on that count while one of them dies — comes later. This was about making sure the foundation was solid first, so that when consensus bugs show up, I know they're consensus bugs.
