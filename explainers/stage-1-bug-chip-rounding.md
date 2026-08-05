# The chip-rounding bug

*Found while testing [Stage 1](stage-1-counting-engine.md).*

This is the most interesting thing in the project so far, and it's worth being able to tell properly.

## The setup

The limiter tracks partial amounts — "you have 4.7 requests left." Computers can't store fractions perfectly, the same way you can't write 1/3 as a decimal without stopping somewhere.

That's normally harmless. Everyone knows about it, nobody cares, the error is a rounding hair.

## The problem

**Different chip types round that hair at different moments.**

Apple/ARM chips do a particular piece of this math in one combined step. Intel/AMD chips do it in two steps and round in the middle. Same code, same input, answers that differ in the last decimal place.

Still sounds harmless. Here's why it isn't:

**This project's entire promise is that three copies always hold *exactly* the same numbers.** A hair apart *is* a disagreement. If two of the three machines happened to run on different chip types, they'd quietly drift apart — and the test that proves the whole project works ("all nodes identical") would start failing.

Worse: it wouldn't fail reliably. It'd fail *sometimes*, buried in the consensus logic — the most complex and most timing-sensitive part of the codebase, and the last place anyone wants to be hunting a bug that isn't actually there.

## How bad, actually

I measured it rather than guessing: across 5 million realistic inputs, the two chip types disagree **about 1 time in 5.**

Not a rare edge case. Routine.

## How it got found

Not by being clever — by not trusting my own work.

I'd written a confident explanation in a code comment about why the math was arranged a certain way. Before leaving it there, I checked whether the claim was actually true.

**It wasn't.** So I dug further, and found the real issue underneath it.

Along the way I ran a check that reported the problem didn't exist — 20 million samples, zero disagreements, apparently conclusive. That result was wrong too: the exact effect I was hunting for was also quietly affecting my measurement, so I was comparing the broken version against itself. Once the measurement was fixed, "0%" became "21.5%."

The lesson I took from that: **when a result clears you of a problem you suspected, be more suspicious of the experiment than relieved by the answer.** Especially a suspiciously clean zero.

## The fix

One line, forcing every chip to round at the same moment.

Then — and this is the part that matters — **I deliberately broke it again to confirm the new test caught it.** A regression test you've never seen fail is just a comment that takes longer to run. It failed exactly as intended, so the fix can't quietly come undone later.

## Why this is worth telling

It isn't "I built a feature." It's:

- I found a bug **in the layer below the one I was working on**
- I found it by **distrusting my own explanation** rather than by hitting a symptom
- I **measured** the impact instead of estimating it — twice, because the first measurement lied
- I fixed it **before** it got built underneath the hardest part of the system, where it would have been brutal to diagnose
- I **proved the guard works** by breaking it on purpose

That's the difference between someone who writes code and someone who thinks about what a system actually guarantees.

It's also honest about a real failure mode: I wrote something confidently wrong, and my first attempt to verify it produced a reassuring answer that was also wrong. Catching both took deliberately re-checking work that already looked finished.

## If an interviewer asks

**"How did you even think to look for that?"**
I didn't go looking for it. I'd written a claim in a comment and wanted to confirm it before it became something someone trusted. The claim was wrong, and the real problem was sitting underneath it.

**"Would this have actually broken in production?"**
Only with mixed chip types across the cluster — all three nodes on the same hardware and it never surfaces. But "we happen to be safe right now" isn't the same as correct, and cluster hardware is exactly the kind of thing that changes without anyone thinking about it.

**"Isn't a fraction of a token too small to matter?"**
For rate limiting, yes, completely. For a system whose entire claim is that replicas hold identical state, no — identical is a binary. It either matches or it doesn't.
