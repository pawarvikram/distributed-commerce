# Syllabus coverage

The 31 chapters preserve the agreed technology-focused course. The separate DSA track is excluded by user instruction. Angular/TypeScript and optional search remain secondary extensions; no React track is added.

| Area | Chapters | Coverage |
|---|---|---|
| Foundations and Java |1–3|Partial failure, modern Java, JVM, concurrency, bounded work|
| Spring and APIs |4–5|Lifecycle/proxies/transactions; HTTP and integration choices|
| Relational data |6|JPA, SQL, constraints, isolation, locks, plans, recovery|
| Kafka core |7–10|Log/storage/KRaft, producer, consumer, ordering and transactions|
| Kafka applications |11–15|Spring listeners, outbox/inbox/Saga, schemas, Connect/Streams, operations/security/DR|
| Data and boundaries |16–19|MongoDB, Redis, DDD/CQRS/event sourcing, networking/Gateway|
| Platform |20–22|Docker, Kubernetes/Helm, AWS/Azure/Terraform, cost/recovery|
| Production engineering |23–27|Security, observability/SLOs, testing/CI/CD, theory, performance|
| Design and evidence |28–31|HLD, LLD/patterns, 25 failure labs, interviews/portfolio|

## One project

Customer, Catalog, Order, Inventory, Payment, Shipping, Notification, Audit/Reporting, and Gateway responsibilities remain within one commerce architecture. Implement progressively: runnable local application, database ownership, Kafka, reliability, projections/caching, deployment, security/observability, then failure/performance evidence.

Scoped examples and practice designs are documented. This does not claim every service, module, deployment, or test is implemented. Practice briefs become portfolio evidence only after implementation and review. Reading alone does not establish professional experience or mastery.
