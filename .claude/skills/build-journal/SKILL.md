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
