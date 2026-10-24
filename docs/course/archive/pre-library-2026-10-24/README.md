# Technology course and notes

Project: C:\Projects\distributed-commerce
Restarted at Chapter 1 on 2026-10-24. No dedicated DSA track.
The 16-week schedule is a pacing baseline; chapters are smaller teaching units, not one-to-one weeks.

- [Reusable teaching prompt](teaching-prompt.md)
- [Chapter 1 full notes](chapters/01-foundations/notes.md)
- [Chapter 1 revision sheet](chapters/01-foundations/revision.md)
- [Source ledger](source-ledger.md)
- [Progress](progress.md)

## Chapter index

| Chapter | Coverage | Status |
|---|---|---|
| 1 | Distributed foundations; monoliths; partial failure; REST/JMS/Kafka; events; Kafka architecture; Java/Spring roles | Learning; notes created |
| 2 | Java 21 features; JVM memory, loading, JIT, GC, JFR, dumps, profiling | Not started |
| 3 | Java Memory Model; concurrency primitives, collections, executors, futures, virtual threads, backpressure | Not started |
| 4 | Spring IoC, lifecycle, auto-configuration, proxies, MVC/WebFlux, transaction boundaries, async, shutdown | Not started |
| 5 | HTTP/REST, OpenAPI, gRPC/Protobuf, GraphQL, WebSockets/SSE, webhooks, JMS/AMQP, integration choices | Not started |
| 6 | JPA/Hibernate, PostgreSQL, MVCC/isolation, locks, SQL/indexes/plans, migrations, replicas/sharding, backup | Not started |
| 7 | Kafka topics, keys, partitions, offsets, retention/replay, KRaft, deployment, broker storage/replication | Not started |
| 8 | Producer metadata, batching, compression, buffers, acknowledgments, retries, timeouts, fencing | Not started |
| 9 | Consumer groups, offsets/commits, rebalances/protocols, assignment, threading, lag, share groups comparison | Not started |
| 10 | Kafka ordering/partitioning, delivery guarantees, transactions, isolation, failure recovery | Not started |
| 11 | Spring Kafka factories/listeners, concurrency, acknowledgment, errors, deserialization, retries/DLT, tests | Not started |
| 12 | Outbox/inbox, business idempotency, Saga, compensation, CDC, database/Kafka consistency | Not started |
| 13 | Schemas, JSON/Avro/Protobuf, registries, compatibility, ownership, evolution | Not started |
| 14 | Kafka Connect and Streams; connectors/tasks/offsets; KTable/joins/windows/state/recovery | Not started |
| 15 | Kafka tuning, monitoring, security, operations, reassignment, cross-cluster/multi-region recovery | Not started |
| 16 | MongoDB modeling/indexes/aggregation/transactions/concerns/replication/sharding; optional search | Not started |
| 17 | Redis structures, TTL/eviction, cache patterns/invalidation/stampedes, locks/leases, HA/recovery | Not started |
| 18 | DDD, bounded contexts, microservices/modular monolith, ownership, CQRS/event sourcing, API composition | Not started |
| 19 | DNS/TCP/TLS/HTTP, proxy/load balancing, pooling, Gateway, discovery, limits/deadlines | Not started |
| 20 | Docker images/layers/builds/networking/storage/resources/security, Compose, Java containers | Not started |
| 21 | Kubernetes control plane/workloads/networking/storage/probes/RBAC/scaling/rollouts; Helm | Not started |
| 22 | AWS/Azure architecture, networking/IAM/managed services/zones/regions, Terraform, cost/recovery | Not started |
| 23 | Spring Security, OAuth/OIDC/JWT, authorization, service identity, TLS/mTLS, secrets, OWASP/threat models | Not started |
| 24 | Logs/metrics/traces, Actuator/Micrometer/OTel/Prometheus/Grafana, SLI/SLOs, incident diagnosis | Not started |
| 25 | JUnit 6/Mockito/Testcontainers, unit/integration/contract/E2E/failure tests, Maven/CI/CD/releases | Not started |
| 26 | Distributed theory: CAP/PACELC, consistency/quorums/Raft/clocks/2PC, retries/bulkheads/fencing | Not started |
| 27 | Performance: queueing/Little's Law, pools/latency/throughput, profiling, load/stress/benchmarks | Not started |
| 28 | HLD: requirements, estimates, contracts/data, reliability, scaling, ADRs, all agreed practice designs | Not started |
| 29 | LLD: SOLID/UML/patterns, state machines, concurrency-safe design, all agreed practical exercises | Not started |
| 30 | The 25 agreed failure labs; runbooks, recovery, reconciliation, disaster recovery evidence | Not started |
| 31 | Technical/behavioral communication, technology mock interviews, revision, final project documentation | Not started |

All detailed subtopics and exercises in the agreed master syllabus remain in scope. Chapters can span sessions. Testing, security basics, documentation, and observability begin early rather than waiting for their dedicated chapter. OS/Linux/Git foundations are introduced where needed. No empty chapter is marked complete.

## Project progression
Foundation -> Order REST -> PostgreSQL -> Kafka -> outbox/inventory/payment Saga -> shipping/notifications/reporting -> MongoDB/Redis -> Gateway -> Compose/Kubernetes -> security/observability -> failure/performance validation.
