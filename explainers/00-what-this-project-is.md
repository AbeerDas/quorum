# What this project is

## The 30-second version

Think of an API as a club with a bouncer at the door. If one customer starts hammering that door — thousands of requests a second, whether that's an attack or just a buggy script someone deployed by accident — the bouncer needs to say "you've had enough, wait a bit," **without** ruining the night for everyone else in line.

The twist that makes this a real engineering problem: **the bouncer can't become the new weak point.** If the one bouncer keels over, the door has no protection at all. You've moved the problem, not solved it.

So the actual goal is: **three bouncers who always agree on the count, and if one collapses, the other two take over instantly** — no gap in coverage, no lost count, no double-counting, no human paged at 3am.

## Why that's the hard part

Getting three separate machines to agree on a number, while any one of them might die at any moment, is one of the genuinely difficult problems in computing. It has a name — **consensus** — and the standard solution is an algorithm called **Raft**.

Real infrastructure runs on this. Kubernetes, etcd, and Consul all depend on Raft to keep their cluster state consistent. It is not an academic exercise.

Most people who need this reach for an existing library. **This project implements Raft from scratch**, which is the entire point: it's the difference between "I configured a tool that does the hard thing" and "I understand the hard thing."

## What the rate limiting is really for

The rate limiter is the excuse, not the point.

Counting requests is straightforward. Counting requests *identically across three machines while one of them is dying* is not. The rate limiter is a small, easy-to-explain problem to hang the difficult distributed-systems work on — you can describe what it does in one sentence, which leaves all the conversation for the interesting part.

There's a dashboard too, which makes the failover visible — you click "kill the leader" and watch a replacement get elected in real time. **That's a demo, not the project.** If someone's first impression is "rate limiting dashboard," the work has been badly described.

## The one-sentence version

> A fault-tolerant distributed rate limiter in Go, using a hand-written Raft consensus protocol to replicate state across three nodes with automatic leader election and failover.

## If an interviewer asks

**"Why not just use Redis?"**
Redis would work, and for a real product it'd probably be the right call. But then the interesting part — keeping replicas consistent through a failure — is happening inside someone else's code, and I'd have nothing to say about it. The point here was to build that part.

**"Why not use an off-the-shelf Raft library?"**
Same reason. `hashicorp/raft` is excellent and I'd use it in production. Writing it myself is what turns "I know Raft exists" into "I know why leader election needs randomized timeouts."

**"Isn't rate limiting kind of trivial?"**
The counting is. Making three machines agree on the count while one is failing is not — and that's what most of the code and nearly all of the tests are about.
