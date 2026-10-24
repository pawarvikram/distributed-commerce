# Source ledger

Verified 2026-10-24. PostgreSQL 18 documentation is used as an explicit reference baseline, not an installation decision. Jakarta Persistence 3.2 and Hibernate guide 7.1.36.Final explain concepts; they are not asserted as the dependency versions selected by proposed Boot 4.1.1. Live Spring Data JPA is 4.1.2. Java 21/Maven remain agreed.

| ID | Official source | Sections |
|---|---|---|
| D1 | [MVCC introduction](https://www.postgresql.org/docs/18/mvcc-intro.html) | Snapshots |
| D2 | [Transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html) | Read committed, repeatable read, serializable |
| D3 | [Explicit locking](https://www.postgresql.org/docs/18/explicit-locking.html) | Row locks, deadlocks |
| D4 | [Constraints](https://www.postgresql.org/docs/18/ddl-constraints.html) | Check, unique, foreign keys |
| D5 | [Multicolumn indexes](https://www.postgresql.org/docs/18/indexes-multicolumn.html) | B-tree key usage |
| D6 | [Using EXPLAIN](https://www.postgresql.org/docs/18/using-explain.html) | Estimates, actual execution, buffers |
| D7 | [Routine vacuuming](https://www.postgresql.org/docs/18/routine-vacuuming.html) | Dead tuples, statistics, wraparound |
| D8 | [Backup and restore](https://www.postgresql.org/docs/18/backup.html) | Backup approaches |
| D9 | [PITR](https://www.postgresql.org/docs/18/continuous-archiving.html) | Base backup plus WAL, recovery |
| D10 | [Standby servers](https://www.postgresql.org/docs/18/warm-standby.html) | Streaming replication and lag |
| D11 | [Table partitioning](https://www.postgresql.org/docs/18/ddl-partitioning.html) | Declarative partitions, pruning |
| D12 | [CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html) | Concurrent builds and caveats |
| D13 | [Jakarta Persistence 3.2](https://jakarta.ee/specifications/persistence/3.2/jakarta-persistence-spec-3.2.html) | Entity lifecycle, versions, persistence contexts |
| D14 | [Hibernate ORM guide](https://docs.hibernate.org/orm/7.1/userguide/html_single/) | Fetching, flushing, batching |
| D15 | [Spring Data transactionality](https://docs.spring.io/spring-data/jpa/reference/jpa/transactions.html) | Service/facade transaction boundaries |

Documented mechanisms are cited. Commerce schemas, lock ordering, migration sequencing, and capacity reasoning are course recommendations. No claimed book consultation or measured database results.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed versions, executions, or measured results are claimed. Verified date: 2026-10-24.

