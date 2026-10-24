# Chapter 8 revision

- Enqueue != acknowledgment != business completion.
- Partition key groups related work; eventId identifies an event.
- Application resend is different from producer protocol retry.
- Buffer, request, and delivery timeouts cover different waits.
- acks=all works with ISR/minimum ISR; it does not wait for consumers.
- A stable outbox event plus business uniqueness survives ambiguous publication.

Retrieval: Draw the seven-step send path. Explain a lost acknowledgment. Calculate backlog storage for your own workload. Diagnose buffer exhaustion using metrics.

Mastery checklist: [ ] explain [ ] draw [ ] run demo [ ] inject failed response [ ] defend key [ ] compare settings [ ] answer assessment. No mastery recorded yet.

