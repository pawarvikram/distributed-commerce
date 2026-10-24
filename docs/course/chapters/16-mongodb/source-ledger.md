# Source ledger

All links opened 2026-10-24. MongoDB manual URLs are rolling documentation; no server version is selected or installed by this chapter. Verify exact feature support against the eventual server and driver. Live Spring Data MongoDB reports 5.1.2; it does not silently replace the proposed Boot 4.1.1 dependency set. Java 21 and Maven remain the course baseline.

| ID | Official source | Sections |
|---|---|---|
| M1 | [Data modeling](https://www.mongodb.com/docs/manual/data-modeling/) | Document design and access patterns |
| M2 | [Modeling practices](https://www.mongodb.com/docs/manual/data-modeling/best-practices/) | Embedding, references, bounded documents |
| M3 | [Compound indexes](https://www.mongodb.com/docs/manual/core/indexes/index-types/index-compound/) | Field order, prefixes |
| M4 | [Aggregation](https://www.mongodb.com/docs/manual/aggregation/) | Pipeline stages |
| M5 | [Transactions](https://www.mongodb.com/docs/manual/core/transactions/) | Single-document atomicity, distributed transactions |
| M6 | [Read concern](https://www.mongodb.com/docs/manual/reference/read-concern/) | Read guarantees |
| M7 | [Write concern](https://www.mongodb.com/docs/manual/reference/write-concern/) | Acknowledgment, majority, timeouts |
| M8 | [Replication](https://www.mongodb.com/docs/manual/replication/) | Replica-set roles and elections |
| M9 | [Sharding](https://www.mongodb.com/docs/manual/sharding/) | Routing and shard keys |
| M10 | [Change streams](https://www.mongodb.com/docs/manual/changestreams/) | Resume and deployment requirements |
| M11 | [MongoDB Search](https://www.mongodb.com/docs/search/) | Search capability overview |
| M12 | [Spring sessions/transactions](https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html) | Session and transaction manager |
| M13 | [Unique indexes](https://www.mongodb.com/docs/manual/core/index-unique/) | Uniqueness and deployment limitations |
| M14 | [Explain](https://www.mongodb.com/docs/manual/reference/method/db.collection.explain/) | Execution statistics |

Mechanisms below are documented facts. The order-view design, owner-version policy, rebuild sequence, and workload estimates are original recommendations. No book claims or performance measurements.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed versions, executions, or measured results are claimed. Verified date: 2026-10-24.

