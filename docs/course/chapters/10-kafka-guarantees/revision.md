# Chapter10 revision

- State the scope of every delivery guarantee.
- Producer idempotence, Kafka transactions, and business idempotency solve different problems.
- read_committed filters aborted/uncommitted transactional data; it does not deduplicate business duplicates.
- Put input progress and Kafka output in the same Kafka transaction when implementing atomic transformations.
- PostgreSQL and HTTP effects require their own recovery design.

Retrieve: explain fencing, stable-offset visibility, partition-key migration, and a lost commit response. Draw the external-effect failure window.

Checklist: [ ] mechanism [ ] diagram [ ] failure exercise [ ] transaction lab [ ] assessment. Execution pending.

