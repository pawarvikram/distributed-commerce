# Course checkpoint

Updated: 2026-10-24 (Asia/Calcutta).

## Scope

Technology-focused Java backend and distributed systems course. No dedicated DSA track.
User approved current Spring Boot with managed JUnit 6 instead of the original JUnit 5 requirement.

## Progress

- A: Initial professional assessment delivered; mastery remains unverified.
- B: Sixteen-week roadmap delivered.
- C: Core dependency baseline documented below; local integration verification pending.
- D/E: Architecture and initial low-level design delivered in chat; implementation pending.
- F: Setup guide and Kafka Compose file created; Java verified; Maven/Docker installation and Kafka execution pending.
- G/I: All 31 detailed chapter notes, revision sheets, source ledgers, complete book, and 310 study answers are saved in docs/course. Chapter 1 remains Learning; learner assessment and later mastery remain unverified.
- H: Runnable Maven/service implementation and runtime labs remain pending; embedded examples are unexecuted.

## Core baseline

These are documentation-verified selections, not executed integration-test results.

| Component | Selection | Ownership |
|---|---|---|
| Java | Java 21; distribution/patch pin pending | Project |
| Spring Boot | 4.1.1 | Parent POM |
| Spring Framework | 7.0.9 | Boot |
| Spring Kafka | 4.1.1 | Boot |
| Kafka client | 4.2.1 | Boot |
| Kafka broker | 4.2.2, KRaft | Compose; official 4.2 quickstart image |
| Spring Cloud | 2025.1.3 | Cloud BOM, added when Gateway is introduced |
| Spring Data JPA / Redis | 4.1.1 | Boot |
| Spring Data MongoDB | 5.1.1 | Boot |
| Hibernate ORM | 7.4.5.Final | Boot |
| Testcontainers | 2.0.5 | Boot |
| JUnit Jupiter | 6.0.3 | Boot; user approved |
| Mockito | 5.23.0 | Boot |
| Maven | 3.10.0 | Wrapper selection |

PostgreSQL 18, MongoDB 8.0, and Redis 8 are proposed server families. Exact patch images, digests, driver/server integration checks, and Kubernetes tooling pins remain pending. No floating tags should enter runnable manifests.

## Architecture decisions (proposed)

- Order Service owns persisted Saga workflow state.
- Services communicate business commands/results asynchronously through Kafka.
- Local state transitions and their outgoing outbox records commit in one database transaction.
- Consumer deduplication, business mutation, and outgoing outbox records share a local transaction.
- Business identifiers and database constraints enforce business idempotency beyond event-ID deduplication.
- Inventory reservations are atomic in PostgreSQL; Redis is not inventory authority.
- No cross-service database writes or foreign keys.
- The first orientation uses one application and one Kafka container, with explicit durability limitations.

## Environment observations

- Historical initial workspace contained only .git; the project now resides at C:\Projects\distributed-commerce with the complete notes library.
- Java launcher and Git found on PATH.
- Direct Java and javac probes succeeded: Oracle JDK 21.0.10 at C:\Program Files\Java\jdk-21.0.10. The default PATH uses Oracle javapath shims.
- Maven, Docker, kubectl, and Helm not found on the current process PATH. This does not prove they are absent from the computer.
- No application has been created, built, or tested.

## Next action

Open docs/course/README.md or docs/course/book.md. Begin Chapter 1 and record the learner assessment. Follow docs/windows-environment.md when implementing the first runnable milestone. Do not mark examples tested or topics mastered merely because notes exist.

