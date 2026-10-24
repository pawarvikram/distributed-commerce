# Chapter 28 revision

Start with user action -> invariant -> workload -> API/data ownership -> mechanism -> failure -> scaling -> alternatives.

- Acceptance and completion are different latencies.
- An estimate needs units and visible assumptions.
- An idempotency key requires durable enforcement and conflict behavior.
- Trace every arrow for crash-before/crash-after outcomes.
- Database ownership is a correctness boundary.
- Replication, backup, and restoration solve different problems.
- A hot product can be a bottleneck at low global RPS.

Practice: design URL shortener, ticket booking, and scheduler using the table in the notes; give two alternatives each. Rehearse a lost-response and regional-failure follow-up.

Mastery evidence: annotated HLD, capacity worksheet, API contracts, ADR, failure trace, and independent explanation. Status: material available; learner mastery unassessed.

