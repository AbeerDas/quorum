# Stage 2 — the front door

**Tagged in git as `v0.3-single-node-api`.**

## What got built

[Stage 1](stage-1-counting-engine.md) built the counting engine, but nothing could reach it — it was an engine on a stand with no car around it. Stage 2 is the car: a real web server that accepts real requests over the internet and answers them.

You can now start this thing up and talk to it.

## The three doors

**"May I make this request?"** — the main one. A caller asks, and gets back either "yes, and you have 2 left" or "no, try again in 20 seconds." The refusal is a proper `429 Too Many Requests`, which is the standard the entire web already agrees means "slow down" — so existing tools and libraries handle it correctly without being taught anything.

**"How are you doing?"** — a status page reporting what limit is in force, how many requests have been allowed, how many blocked, and how many callers are being tracked.

**"Change the limit."** — adjust the rules while it's running, without restarting or dropping traffic. The dashboard will eventually use this so you can slide the limit up and down and watch blocking kick in sooner or later.

## An honest decision worth explaining

The status page has fields describing the cluster — which machine is in charge, which others it can see. **There is no cluster yet.** So the page plainly reports `"mode": "single-node"`, no known peers, and zero elections held.

The tempting alternative was to leave those fields blank or make something up. Both are worse: blank means the page changes shape later and the dashboard breaks, and making something up means the system lies about itself. Reporting real, honest values in the final shape means nothing has to be rewritten later and nothing is pretending.

There's a test that specifically enforces this, so nobody can quietly turn it into a fake cluster.

## How it was proven to work

Same discipline as before — tests written first, watched fail, then made to pass. **13 tests** covering this layer (25 counting the table-driven variations), on top of Stage 1's 16.

The ones worth mentioning:

**Fairness holds through the front door too.** Stage 1 proved one noisy caller can't affect another *inside the engine*. This proves it survives the trip through the web server as well — the promise holds end to end, not just in the part that was convenient to test.

**Under 300 simultaneous requests against a limit of 50, exactly 50 got through** — and every single request was accounted for as either allowed or blocked, with none lost. Run under Go's race detector.

**Bad input can't break it or corrupt the count.** Missing fields, malformed data, wrong types, wrong request methods — all rejected cleanly with a clear error message. Critically, a malformed request is *not* counted as a rate-limit decision, so garbage traffic can't distort the numbers.

**A rejected settings change leaves the old settings untouched.** Try to set a nonsense limit and it's refused *and* the previous limit is still exactly what it was — no half-applied state.

**Time is controllable in tests.** The tests can fast-forward the clock to prove tokens refill, instead of actually sleeping. That keeps them instant and, more importantly, reliable — tests that depend on real timing are the classic source of "it failed in CI but passes on my machine."

Beyond the automated tests, I started the real server and hit it with real requests from the command line: three allowed, the fourth refused with a correct 20-second retry hint, a second caller unaffected, settings changed live, bad input rejected. Verified by looking at actual output, not assumed.

## Also worth noting

Your **disk was completely full** during this stage — 202MB free out of 228GB — which broke the test run partway through. That wasn't caused by this project. I cleared a rebuildable cache to finish the work, but you'll want to free up real space soon; it will keep interrupting things.

## If an interviewer asks

**"Why 429 instead of just returning an error?"**
`429 Too Many Requests` is the standard status code for rate limiting, and I also send the `Retry-After` header alongside it. That means HTTP clients, load balancers, and retry libraries already know how to behave correctly — they'll back off on their own. Inventing a custom error would mean every caller needs custom handling.

**"Why does the status page say 'single-node' instead of just leaving it blank?"**
Because the shape stays the same once the cluster is real, so nothing downstream has to be rewritten — and because a system that misreports its own state is worse than one that admits what it isn't. There's a test enforcing it.

**"How do you test something that depends on time passing?"**
The clock is supplied to the server rather than read from inside it, so tests can fast-forward instead of sleeping. Tests that wait on real time are slow and flaky. This also mattered for a deeper reason: it's the same design that lets three machines replay identical history later and land in identical states.

**"What's still missing?"**
Speed measurements, which are coming with the monitoring stage so they get built once and properly. And the actual point of the project — three of these agreeing with each other — which is next.
