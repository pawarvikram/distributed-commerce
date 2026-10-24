# Source ledger

Opened 2026-10-24. Redis links are rolling `latest` documentation; no Redis server release is selected by this chapter. Commands below avoid newer compare/delete additions whose availability would need an explicit version decision. Spring Data Redis live docs report 4.1.2; proposed Boot remains 4.1.1. Java remains 21 and Maven the build tool.

| ID | Official source | Sections |
|---|---|---|
| R1 | [Data types](https://redis.io/docs/latest/develop/data-types/) | Strings, hashes, sets, lists, sorted sets, streams |
| R2 | [Eviction](https://redis.io/docs/latest/develop/reference/eviction/) | maxmemory and policies |
| R3 | [EXPIRE](https://redis.io/docs/latest/commands/expire/) | TTL semantics |
| R4 | [SET](https://redis.io/docs/latest/commands/set/) | NX and expiration options |
| R5 | [Distributed locks](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/) | Tokens, expiry, correctness caveats |
| R6 | [Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) | RDB/AOF trade-offs |
| R7 | [Replication](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/) | Asynchronous replication |
| R8 | [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) | Monitoring and failover |
| R9 | [Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) | Slots, hash tags, failover constraints |
| R10 | [Lua scripting](https://redis.io/docs/latest/develop/programmability/eval-intro/) | Atomic execution and script lifetime |
| R11 | [Spring Redis cache](https://docs.spring.io/spring-data/redis/reference/redis/redis-cache.html) | Cache manager, TTL, serialization |
| R12 | [Pub/Sub](https://redis.io/docs/latest/develop/pubsub/) | Delivery semantics |

Documented behavior is cited. Cache policies, fencing examples, business boundaries, and arithmetic are course recommendations. The first attempted lock-document URL failed; the canonical clients/patterns URL above was successfully opened. No benchmark or book-reading claim.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed versions, executions, or measured results are claimed. Verified date: 2026-10-24.

