# Course source ledger

Prepared/checked 2026-10-24. Official documentation, primary specifications/research, and explicitly identified primary engineering/academic references support the notes. Chapter ledgers record versions and access limitations. No unavailable book content is claimed consulted.

## Versions and scope

The [preserved baseline ledger](baseline-source-ledger.md) records earlier selections. Java 21, proposed Boot 4.1.1, Kafka client 4.2.1/broker 4.2.2, and user-approved managed JUnit 6 remain the teaching baseline, not newly resolved dependency results. Live Spring documentation has advanced. Kafka 4.2 pages now advertise 4.3, and their API banner says 4.2.0. Reverify exact compatibility before implementation.

Kafka-core references are Apache-owned. Spring integration, schema formats, and connectors use their respective official owners; these are distinct source categories. Documented facts, project recommendations, illustrative calculations, expected observations, and actual measurements must remain separate. No runtime measurements are invented.

## Chapter ledgers

- [Chapter 1 — Foundations of Distributed Systems: Java 21, Spring Boot, REST APIs, Messaging, and Why Apache Kafka Exists](chapters/01-foundations/source-ledger.md)
- [Chapter 2 — Java 21 and the JVM: From Domain Types to Production Evidence](chapters/02-java-jvm/source-ledger.md)
- [Chapter 3 — Concurrency, the Java Memory Model, and Bounded Work](chapters/03-concurrency/source-ledger.md)
- [Chapter 4 — Spring Internals: Objects, Proxies, Requests, and Transactions](chapters/04-spring-internals/source-ledger.md)
- [Chapter 5 — APIs and Integration Contracts](chapters/05-api-integration/source-ledger.md)
- [Chapter 6 — PostgreSQL, JPA, and Durable Business Invariants](chapters/06-postgresql-jpa/source-ledger.md)
- [Chapter 7 — Kafka Architecture, Storage, and Replication](chapters/07-kafka-architecture/source-ledger.md)
- [Chapter 8 — Kafka producers: from a method call to a durable record](chapters/08-kafka-producer/source-ledger.md)
- [Chapter 9 — Kafka consumers, offsets, groups, and rebalances](chapters/09-kafka-consumer/source-ledger.md)
- [Chapter 10 — Ordering, delivery guarantees, and Kafka transactions](chapters/10-kafka-guarantees/source-ledger.md)
- [Chapter 11 — Spring Kafka integration and recoverable listeners](chapters/11-spring-kafka/source-ledger.md)
- [Chapter 12 — Outbox, inbox, business idempotency, and Saga orchestration](chapters/12-outbox-saga/source-ledger.md)
- [Chapter 13 — Event schemas, compatibility, and independent deployments](chapters/13-event-schemas/source-ledger.md)
- [Chapter 14 — Kafka Connect and Kafka Streams](chapters/14-connect-streams/source-ledger.md)
- [Chapter 15 — Kafka operations, security, capacity, and disaster recovery](chapters/15-kafka-operations/source-ledger.md)
- [Chapter 16 — MongoDB Read Models, Queries, and Recovery](chapters/16-mongodb/source-ledger.md)
- [Chapter 17 — Redis: Caches, Expiration, Leases, and Recovery](chapters/17-redis/source-ledger.md)
- [Chapter 18 — DDD, Service Ownership, and Read/Write Models](chapters/18-microservices-ddd/source-ledger.md)
- [Chapter 19 — Networking, Gateways, Discovery, and Time Budgets](chapters/19-networking-gateway/source-ledger.md)
- [Chapter 20 — Docker: Packaging, Isolation, and Running Java Reliably](chapters/20-docker/source-ledger.md)
- [Chapter 21 — Kubernetes: Reconciliation, Workloads, and Application Recovery](chapters/21-kubernetes/source-ledger.md)
- [Chapter 22 — AWS, Azure, Terraform, Cost, and Disaster Recovery](chapters/22-cloud-terraform/source-ledger.md)
- [Chapter 23 — Security: Identity, Authorization, Tokens, and Trust Boundaries](chapters/23-security/source-ledger.md)
- [Chapter 24 — Observability, SLOs, and evidence-led diagnosis](chapters/24-observability/source-ledger.md)
- [Chapter 25 — Testing, Maven, CI/CD, and safe releases](chapters/25-testing-cicd/source-ledger.md)
- [Chapter 26 — Distributed-systems theory applied to real decisions](chapters/26-distributed-theory/source-ledger.md)
- [Chapter 27 — Performance engineering, queueing, and capacity](chapters/27-performance/source-ledger.md)
- [Chapter 28 — High-Level Design: turning requirements into a defensible system](chapters/28-hld/source-ledger.md)
- [Chapter 29 — Low-Level Design: responsibilities, invariants, and change](chapters/29-lld/source-ledger.md)
- [Chapter 30 — Failure engineering: prove recovery, not just restart](chapters/30-failure-labs/source-ledger.md)
- [Chapter 31 — Technical interviews, engineering communication, and project evidence](chapters/31-interview-evidence/source-ledger.md)
