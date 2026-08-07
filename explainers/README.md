# Explainers

Plain-language write-ups of what this project is and what was actually built at each stage — no jargon, no assumed background.

Each one answers three things: what got built, how it was proven to work, and what's worth saying about it out loud.

## Start here

- [What this project is](00-what-this-project-is.md) — the whole thing in one page

## By stage

- [Stage 1 — the counting engine](stage-1-counting-engine.md)
  - [The chip-rounding bug](stage-1-bug-chip-rounding.md) — found while testing Stage 1
- [Stage 2 — the front door](stage-2-the-front-door.md)
- [Stage 3 — the consensus core](stage-3-consensus.md) — the centerpiece, and the mutation-testing story
- [Stage 4 — putting the counter behind consensus](stage-4-replicated-counting.md) — where it became a real distributed system

New stages get a new file here as they're built. Anything notable found along the way gets its own linked write-up, like the bug above.

For the engineering-facing version of the same lessons — root causes and the rules that came out of them — see [`.claude/skills/build-journal/`](../.claude/skills/build-journal/SKILL.md).
