# quorumgate

## Always align before building

**Every time work starts — a new prompt, a new stage, a change in direction — go back and forth with clarifying questions before writing code.** Do not jump straight to implementation because the request sounds clear. The goal is confirmed shared scope and a shared picture of what "good" looks like, not a fast start.

Concretely, before starting:

1. **Ask, then wait.** Surface the real decisions — scope boundaries, data shapes, tradeoffs, what's explicitly out — as questions with recommended options. Ask about things where the answer changes what gets built, not things with an obvious default.
2. **Say what you think the request means** in your own words, including what you are *not* going to build, and let it get corrected before any code exists.
3. **Re-check against `PRD.md`.** If the request and the PRD appear to disagree, raise it explicitly rather than picking one — the PRD wins on scope, but the user may be deliberately changing course.
4. **Re-align mid-stage** if something surfaces that changes the shape of the work (a spec ambiguity, a dependency that doesn't exist, a design fork). Stop and ask rather than deciding unilaterally and continuing.

A stage that gets built fast in the wrong direction costs more than the questions would have. This rule applies even when the next step looks obvious from the stage plan.

## Explain in plain language

The user owns the decisions on this project but is not deep in Go internals, distributed-systems theory, or low-level details. **Explanations and questions must be understandable without that background.**

- Lead with what it means for the project — what breaks, what it costs, what the user would notice — before any mechanism.
- No unexplained jargon. If a term is genuinely needed, gloss it in plain English the first time.
- When asking the user to choose, describe each option by its **consequence**, not its implementation. "One-line change, already tested, nothing else moves" beats naming the technique.
- Reach for an analogy when it makes the stakes clearer, and keep the precise version available if the user wants it.
- Never make the user decode a wall of technical output to answer a question. Do that reading and hand over the conclusion.

## Write an explainer for every stage

`/explainers/` holds plain-language write-ups of what this project is and what was built at each stage. They are the user's material for explaining the work out loud, and they double as the friendliest documentation in the repo.

**Every completed stage gets a new file there** — `stage-N-<short-name>.md` — covering: what got built, how it was proven to work, and what is worth saying about it. Anything notable found along the way (a real bug, a surprising decision) gets its **own linked file**, like `stage-1-bug-chip-rounding.md`, rather than being buried inside the stage write-up. Add every new file to `/explainers/README.md`.

Rules for these files:

- Plain language throughout, per the section above. Analogies are welcome. A reader with no background should follow every sentence.
- Close each one with an **"If an interviewer asks"** section: the questions this work invites, answered honestly and conversationally.
- Be truthful about what is not built yet and about mistakes made. A candid write-up of a bug found and fixed is worth more than a clean summary, and honesty is what makes the rest credible.
- These are distinct from the `build-journal` skill: the journal is the engineering record (root causes, rules to prevent repeats), while explainers are for a human audience.

## Always close the chat with what was written

**Every response that produces work must end by telling the user what was written and framing it for explaining the work out loud.** The user is tracking this project through these summaries, so a response that just says "done" is a failure.

State plainly which files were created or changed and what each one does, then give the interview-facing version: what this work demonstrates, and how to describe it. Skip the jargon; if something technical matters, translate it.

## Learn from what already went wrong

Use the **`build-journal`** skill (`.claude/skills/build-journal/`). Read it before starting a stage or debugging a failure, and append an entry whenever a wrong assumption, environment gotcha, misleading "pass" signal, or user correction costs time. The point is that no mistake in this build gets made twice.

## Source of truth

**[`PRD.md`](PRD.md) is the source of truth for this project.** It is the complete build spec — architecture, technology choices, API contract, data shapes, and the mandatory stage-gated build plan. If anything here, in the README, or in any skill ever conflicts with `PRD.md`, `PRD.md` wins. Read it in full before doing any work in this repo.

Do not re-derive scope from first principles or from what "seems reasonable" for a rate limiter — `PRD.md` Section 4 and Section 16 are explicit about what's in scope and what's a scope trap. When in doubt, go re-read the relevant section rather than improvise.

## What this project is

A fault-tolerant distributed rate limiter in Go, using a **hand-rolled Raft consensus protocol** (no `hashicorp/raft`) to replicate rate-limit state across three nodes with automatic leader election and failover. A React dashboard visualizes it, but the dashboard is not the point — see `PRD.md` Section 5 on positioning. Never describe this project, in commits, docs, or code comments, as "a rate limiting dashboard."

## Non-negotiable build discipline

This repo is built in the mandatory stages defined in `PRD.md` Section 12. For every stage:

1. **Do not start a stage until the previous stage's gate is fully checked**, including the git commit, tag, and push to GitHub. The stage tags (`v0.1-scaffold` through `v1.0`) are the record that the build actually worked at each checkpoint — skipping ahead defeats the entire point of the gate structure.
2. **Write the test first, watch it fail, then implement** — use the `test-driven-development` skill. This matters most in Stage 3 (hand-rolled Raft): a correctness test that passes without ever having failed proves nothing.
3. **`go test ./...` and `go test -race ./...` must both pass** before any stage is considered done. The race detector is a hard requirement for this codebase, not a nice-to-have — Raft and the limiter both run heavy concurrent/goroutine code.
4. **Verify before claiming completion** — use the `verification-before-completion` skill. Actually run the stage's validation checklist and read real output. Do not assert a stage is done because the code looks right.
5. **Debug Raft issues with `systematic-debugging`**, not guesswork. Election and replication bugs are timing-sensitive and intermittent; a fix that isn't hypothesis-driven is easy to get "working" by accident and wrong in production.
6. **No placeholder values ever** — no `[X]`, `TODO`, `lorem ipsum`, or estimated benchmark numbers in anything that ships. `PRD.md` Section 11 and Section 15 are explicit: benchmark figures must be real measured numbers or the section doesn't get written yet.

## Stage 3 (hand-rolled Raft) gets special treatment

This is the highest-value and highest-risk part of the build (`PRD.md` Section 6, "Why hand-rolled Raft"). Build and test it in isolation from the limiter logic, against the Raft paper's Figure 2. **Do not proceed to Stage 4 until all five correctness tests in `PRD.md` Section 9 pass as real, named, automated tests.** If Raft cannot be made correct, the documented fallback (`PRD.md` Section 16) is to ship through Stage 2 and clearly label it single-node — not to ship a broken cluster with a caveat.

## Explicit non-goals

Do not build any of: disk persistence/WAL, snapshotting, log compaction, multi-Raft, sharding, TLS/auth, a real database, Kubernetes manifests, WebSockets, a gRPC-Web/Envoy proxy. See `PRD.md` Section 16 for why each of these is a scope trap for this build.

## Repo layout

- `/raft` — hand-rolled Raft consensus core (Stage 3)
- `/limiter` — token-bucket limiter and in-memory store (Stage 1)
- `/api` — external REST API (Stage 2), gRPC transport for Raft lives alongside `/raft`
- `/ui` — React/Vite dashboard (Stage 7) — scoped separately, not the project's center of gravity
- `PRD.md` — the build spec and source of truth
- `.claude/skills/` — installed skills supporting this build (see below)

## Installed skills

Installed per `PRD.md` Section 18:

- `find-skills` — search for a skill when a gap shows up mid-build rather than improvising
- `test-driven-development`, `systematic-debugging`, `verification-before-completion`, `using-git-worktrees`, `finishing-a-development-branch` — the discipline layer behind the rules above
- `golang-concurrency`, `golang-testing`, `golang-grpc`, `golang-observability`, `golang-performance`, `golang-error-handling` — Go implementation skills for Stages 1-5
- `docker-architect` — Dockerfile/Compose patterns for Stage 6 (closest available match to the PRD's `docker-compose-architecture`; same repo, different skill name)
- `vite-react-best-practices` — React/Vite patterns for Stage 7
