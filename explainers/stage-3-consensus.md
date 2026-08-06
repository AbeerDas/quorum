# Stage 3 — the consensus core

**Tagged in git as `v0.4-raft-core`.** This is the centerpiece of the project.

## What got built

The thing that makes three machines agree, written from scratch. It's called **Raft**, and it's the algorithm Kubernetes and etcd use internally to keep their clusters consistent. Most people who need this use an off-the-shelf library. Writing it by hand is the whole point — it's the difference between "I configured a tool that does the hard thing" and "I understand the hard thing."

Three machines. One is the leader; the other two follow. If the leader dies, the survivors hold an election and one takes over — automatically, in well under a second, with nothing lost.

## How it works, roughly

**Everyone starts as a follower** waiting to hear from a leader. If a follower hears nothing for long enough, it assumes the leader is dead, declares itself a candidate, and asks the others to vote for it. Win a majority, become the leader.

**The "majority" part is the whole trick.** Two out of three is a majority. One out of three isn't. Because any two majorities must overlap by at least one machine, you can never have two leaders elected at once — some machine would have had to vote twice, which the rules forbid. That single idea is what stops the cluster from splitting into two halves that each think they're in charge.

**One deliberately odd detail:** each machine waits a *random* amount of time before calling an election. Without the randomness, all three time out simultaneously, all three vote for themselves, nobody wins, and they do it again forever. Randomness breaks the tie. It looks like a hack and it's actually essential.

## The part I'd lead with in an interview

I wrote the five correctness tests the spec demanded. **All five passed on the first run.** That should have been the good news.

Instead I treated it as suspicious, because a test that has never failed hasn't proven it can detect anything. So I went through the implementation and **deliberately broke each safety rule, one at a time**, to check that some test would notice.

**Four of the six sabotages went completely undetected.** I could let a machine vote twice in the same election. I could let a machine that had missed data win an election and destroy that data. I could skip the check that keeps logs consistent. Every test stayed green.

The reason was the same for all four: **every test started from a healthy cluster and killed a machine cleanly.** The machines never actually disagreed with each other. But the rules I'd broken exist *only* to resolve disagreement — so they were never executed. My tests covered the happy path and the tidy failures, which is exactly where this algorithm has no interesting bugs.

So I wrote the hard scenarios: a leader cut off from the others that keeps accepting writes it can never finish, a machine that rejoins after missing everything while shouting the highest election number, a minority of two that can still talk to each other and might convince themselves they're in charge. **The suite now catches all six.**

Both numbers are in the README. The 2-of-6 result is more informative than the 6-of-6, and hiding it would waste the most interesting thing that happened.

## How it was proven to work

**17 tests across the consensus work** — 14 for the core logic, 3 for the real networking.

Beyond the five required proofs, the extras cover the properties those five leave open: a machine that missed data can't win an election; a deposed leader's unfinished writes get thrown away rather than resurrected; a leader can't declare old data safe just because it's widely copied (a genuinely subtle rule from the original research paper, and the one most hand-written implementations get wrong).

**On flakiness:** this kind of code depends on timeouts, so tests can fail randomly on a busy machine even when the code is perfect — which would be devastating here, since these tests *are* the proof. So the tests never sleep a fixed guess; they poll until the thing they're waiting for is true. I then ran the entire suite **25 times in a row** under Go's race detector.

That caught a real bug on roughly the 15th run: a test assumed that once a leader exists, the other machines know who it is. They don't — they find out when the first heartbeat arrives, a few milliseconds later. The test was racing that heartbeat. Correct behavior, wrong assumption in the test. **A single green run would have shipped that.**

## Two honest limitations

**Nothing is written to disk.** Real Raft saves a little state so a crashed machine remembers who it voted for; forgetting can, rarely, let two leaders exist. The spec forbids disk writes, so instead a killed machine stays dead for that session — which keeps the guarantee airtight. The dashboard's freeze/unfreeze control is the recoverable one, since a frozen machine keeps its memory.

**The five headline proofs run on a simulated network,** not real connections. That's deliberate: it lets me sever an exact link at an exact moment, which is what makes partition tests reliable rather than a firewall-wrangling exercise. Real networking gets its own tests proving three actual servers elect a leader, replicate, and fail over across real TCP. I'd rather say this openly than let someone discover it.

## If an interviewer asks

**"How do you know your Raft is actually correct?"**
I don't know it's correct — nobody can prove that from tests alone. What I can say is that I checked whether my tests could detect breakage, and initially they mostly couldn't: 4 of 6 deliberately planted safety bugs passed straight through. I fixed the tests until all 6 were caught. That's a much stronger claim than "the tests pass," and it's the honest version.

**"What's the hardest part of Raft?"**
Not the leader election — that part is intuitive. It's the rule that a leader can't consider old data safe just because it's now stored on most machines. It's counterintuitive, it's the one the original paper devotes a whole figure to, and it doesn't show up in casual testing. I test it directly rather than hoping a random scenario stumbles into it.

**"Why not just use an existing Raft library?"**
For production I would — `hashicorp/raft` is battle-tested and I'd be reckless to compete with it. The point here was to understand it. Writing it is what turns "I know Raft exists" into "I know why election timeouts have to be random."

**"What would you do differently with more time?"**
Persist a small amount of state to disk so killed machines can safely rejoin, and add randomized fault injection — running thousands of cycles with connections dropping at random moments, checking the safety properties hold every time. Mutation testing shows my tests catch the bugs I thought to plant; that would catch the ones I didn't.
