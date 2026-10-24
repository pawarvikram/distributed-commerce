# Complete revision collection

[Course index](README.md) · [Full book](book.md)

- [Chapter 1 — Foundations of Distributed Systems: Java 21, Spring Boot, REST APIs, Messaging, and Why Apache Kafka Exists](#chapter-01)
- [Chapter 2 — Java 21 and the JVM: From Domain Types to Production Evidence](#chapter-02)
- [Chapter 3 — Concurrency, the Java Memory Model, and Bounded Work](#chapter-03)
- [Chapter 4 — Spring Internals: Objects, Proxies, Requests, and Transactions](#chapter-04)
- [Chapter 5 — APIs and Integration Contracts](#chapter-05)
- [Chapter 6 — PostgreSQL, JPA, and Durable Business Invariants](#chapter-06)
- [Chapter 7 — Kafka Architecture, Storage, and Replication](#chapter-07)
- [Chapter 8 — Kafka producers: from a method call to a durable record](#chapter-08)
- [Chapter 9 — Kafka consumers, offsets, groups, and rebalances](#chapter-09)
- [Chapter 10 — Ordering, delivery guarantees, and Kafka transactions](#chapter-10)
- [Chapter 11 — Spring Kafka integration and recoverable listeners](#chapter-11)
- [Chapter 12 — Outbox, inbox, business idempotency, and Saga orchestration](#chapter-12)
- [Chapter 13 — Event schemas, compatibility, and independent deployments](#chapter-13)
- [Chapter 14 — Kafka Connect and Kafka Streams](#chapter-14)
- [Chapter 15 — Kafka operations, security, capacity, and disaster recovery](#chapter-15)
- [Chapter 16 — MongoDB Read Models, Queries, and Recovery](#chapter-16)
- [Chapter 17 — Redis: Caches, Expiration, Leases, and Recovery](#chapter-17)
- [Chapter 18 — DDD, Service Ownership, and Read/Write Models](#chapter-18)
- [Chapter 19 — Networking, Gateways, Discovery, and Time Budgets](#chapter-19)
- [Chapter 20 — Docker: Packaging, Isolation, and Running Java Reliably](#chapter-20)
- [Chapter 21 — Kubernetes: Reconciliation, Workloads, and Application Recovery](#chapter-21)
- [Chapter 22 — AWS, Azure, Terraform, Cost, and Disaster Recovery](#chapter-22)
- [Chapter 23 — Security: Identity, Authorization, Tokens, and Trust Boundaries](#chapter-23)
- [Chapter 24 — Observability, SLOs, and evidence-led diagnosis](#chapter-24)
- [Chapter 25 — Testing, Maven, CI/CD, and safe releases](#chapter-25)
- [Chapter 26 — Distributed-systems theory applied to real decisions](#chapter-26)
- [Chapter 27 — Performance engineering, queueing, and capacity](#chapter-27)
- [Chapter 28 — High-Level Design: turning requirements into a defensible system](#chapter-28)
- [Chapter 29 — Low-Level Design: responsibilities, invariants, and change](#chapter-29)
- [Chapter 30 — Failure engineering: prove recovery, not just restart](#chapter-30)
- [Chapter 31 — Technical interviews, engineering communication, and project evidence](#chapter-31)

---

<a id="chapter-01"></a>

# Chapter 1 — Revision notes

Read after the full notes. This sheet is not evidence of mastery.

## Essential distinctions

| Distinction | Remember |
|---|---|
| Receiver outcome vs caller observation | A timeout can coexist with a committed effect |
| Async client vs durable workflow | A future does not persist work |
| Producer acknowledgment vs order completion | Publication and business processing are separate |
| Command vs event | Requested action versus reported occurrence |
| Partition offset vs business ID | Position is scoped to a partition |
| Current position vs committed position | Fetch progress and restart progress can differ |
| Database transaction vs remote effect | A local rollback does not automatically undo a remote authorization |
| Event dedup vs business idempotency | Different messages can request the same business operation |
| Retention vs consumption | Cleanup policy, not ordinary reads, governs stored records |
| Monolith vs microservices | Deployment/ownership choices, not a ladder of engineering maturity |

## Recovery pattern
Stable caller/request key -> durable order -> outbox -> potentially repeated publication -> consumer local dedup + state change + outgoing outbox -> offset commit.
External provider effects require provider support or reconciliation; an inbox alone does not solve every external effect.

## Calculations
Backlog growth = arrival rate - processing rate when arrival exceeds processing.
Drain time = backlog / (recovery processing rate - continuing arrival rate), only if the denominator is positive.
State units and assumptions. These are planning calculations, not measured performance.

## Retrieval practice
1. Draw the response-lost sequence from memory.
2. Explain three meanings of asynchronous.
3. Compare REST, JMS, and Kafka without treating them as identical categories.
4. Draw three partitions and two independent groups.
5. Explain why replaying notifications may have different consequences from rebuilding reporting.
6. Identify every commit/acknowledgment boundary in an order.
7. Explain why the Java map simulation fails after restart.
8. Name the invariant protecting stock across different order IDs.
9. Describe late payment success during cancellation.
10. List telemetry needed to find a stuck order.

## Mastery checklist
- [ ] Explain without memorized wording.
- [ ] Draw the mechanism.
- [ ] Predict failure states.
- [ ] Run/modify the demonstration and explain its limits.
- [ ] Compare alternatives.
- [ ] Discuss capacity and consistency.
- [ ] Answer a fresh scenario.
- [ ] Troubleshoot using evidence.

Status: Learning; no score yet.
Review after actual study: +1 day, +3 days, +7 days, +14 days.
If first studied 2026-10-24, suggested dates are 2026-10-25, 2026-10-27, 2026-10-31, 2026-11-07. These are recommendations, not scheduled automations or completed reviews.


---

<a id="chapter-02"></a>

# Revision sheet

Prepared 2026-10-24. This is a recall aid; use the full [chapter](chapters/02-java-jvm/notes.md) for mechanisms, code, ten explained interview answers, and solved exercises.

## Core model
Model values deliberately; use Java 21's stable type features to make invalid states harder to express. Separate language guarantees, JVM specification, and HotSpot implementation choices. Investigate whole-process memory and correlate runtime evidence with business latency. Repair ownership and lifecycle mistakes before treating tuning flags as a solution.


## Correct these misconceptions
GC does not remove every object that is no longer useful to the business. A record is not recursively immutable. More heap is not always lower latency. A Java source feature available in a later tutorial may be preview or absent in 21. A successful local demonstration does not prove a service's transaction safety. A dump is evidence requiring interpretation, not a diagnosis by itself.


## Retrieval assessment
Unanswered: Order retains completed futures in a map, request latency rises, and its container restarts while heap occasionally drops. Describe how you would distinguish reachable retention, native-memory pressure, and a slow dependency. State which observation would falsify each hypothesis. No score is recorded until an answer and supporting evidence exist.


## Evidence and practice
No Java code or diagnostic command was executed during authoring. Expected program output is `O-1001 lines=1`, followed by `Reserved: R-1001`. Run `java OrderValueDemo.java` with a confirmed Java 21 installation. This single-file exercise does not require or replace the Maven project.

Illustrative diagnostic sequence, unexecuted: `jcmd <pid> help`, then `jcmd <pid> help JFR.start` and the relevant command help before capture. For example, a bounded recording can use `jcmd <pid> JFR.start name=checkout settings=profile duration=60s filename=checkout.jfr`. Inspect command impact and permissions on the actual target. The jcmd manual documents target access and available-command discovery. [J12] There are no measured performance results or claims of application readiness.


Read the [source ledger](chapters/02-java-jvm/source-ledger.md) before making version-specific claims. Explain the mechanism aloud, draw the failure boundary, and state what evidence would falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-03"></a>

# Revision sheet

Prepared 2026-10-24. This is a recall aid; use the full [chapter](chapters/03-concurrency/notes.md) for mechanisms, code, ten explained interview answers, and solved exercises.

## Core model
Start with the invariant, then identify the synchronization and resource boundaries. Use happens-before reasoning for visibility, protect compound transitions, observe task results, and make overload explicit. Separate local cancellation from durable business recovery.


## Correct these misconceptions
Thread-safe components do not automatically compose into a thread-safe workflow. Cheap virtual threads do not create unlimited downstream capacity. Async does not mean durable. A future timeout does not prove an effect stopped. A semaphore bounds permit holders but may leave unlimited waiters. One successful stress run cannot prove absence of a race.


## Retrieval assessment
Unanswered: a service uses virtual threads, a 20-connection pool, and no admission limit. Payment latency increases fivefold. Explain the possible queue locations, retained resources, duplicate-effect risks, and a bounded recovery design. Name the measurements that would demonstrate improvement.


## Evidence and practice
Source documentation was checked; the Java program was not compiled or run. Expected output: `accepted=10, remaining=0`. It verifies only finite, single-process locking if executed. It does not verify fairness, throughput, distributed stock, cancellation, or durable recovery. No JUnit suite, Maven build, or environment installation was performed.


Read the [source ledger](chapters/03-concurrency/source-ledger.md) before making version-specific claims. Explain the mechanism aloud, draw the failure boundary, and state what evidence would falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-04"></a>

# Revision sheet

Prepared 2026-10-24. This is a recall aid; use the full [chapter](chapters/04-spring-internals/notes.md) for mechanisms, code, ten explained interview answers, and solved exercises.

## Core model
Trace object ownership, proxy crossings, execution threads, and physical resource transactions. Use auto-configuration as explainable conditional assembly. Keep business state and outgoing obligations durable, and make startup and shutdown part of the architecture.


## Correct these misconceptions
An annotation does not execute itself. A bean is not automatically thread-safe. An async task is not a durable event. A transaction does not include arbitrary remote resources. Reactive return types do not transform blocking calls. Graceful shutdown is not a replacement for crash recovery. Boot conventions still need inspection when dependencies change.


## Retrieval assessment
Unanswered: a singleton OrderService stores the current request in a field, invokes a private transactional helper, and calls an async payment method on itself. Identify three distinct mechanisms that fail to match the developer's expectation and propose a corrected boundary design.


## Evidence and practice
References were opened and version banners recorded. The single-file Java example, Maven build, Spring contexts, transaction integration, and shutdown behavior were not executed. Treat the code as a mechanism demonstration. A future implementation needs integration tests proving rollback, proxy traversal, outbox atomicity, and restart behavior on the selected database and reviewed dependency set.


Read the [source ledger](chapters/04-spring-internals/source-ledger.md) before making version-specific claims. Explain the mechanism aloud, draw the failure boundary, and state what evidence would falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-05"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/05-api-integration/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Specify semantics before transport. Separate resource creation, durable acceptance, and completed business work. Preserve identity across retries, define compatibility and error behavior, budget deadlines, and make stream/callback recovery explicit.


## Correct these misconceptions
REST is not simply a naming convention for controllers. HTTP success is not workflow completion. Idempotency is not deduplication in a process-local map. gRPC does not remove partial failure. GraphQL does not guarantee cheap queries. A live socket is not durable delivery. Broker acknowledgment does not imply a downstream business transaction finished.


## Retrieval assessment
Unanswered: a payment callback arrives twice, an order-status stream disconnects, and the original order POST response was lost. Explain the independent recovery mechanism at each boundary, including durable identity, state transitions, and the customer's eventual view.


## Evidence and practice
The OpenAPI example was not parsed, generated, or served. No HTTP/gRPC/GraphQL server or broker was started. Sources were checked on 2026-10-24. Expected observations are design predictions. A future implementation must test duplicate concurrent POSTs, lost responses, payload mismatch, authorization, malformed input, deadline expiry, and schema compatibility.


Consult the [source ledger](chapters/05-api-integration/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-06"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/06-postgresql-jpa/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Enforce invariants where concurrent writers meet. Understand snapshots, locks, and transaction scope; inspect actual SQL and plans; preserve business identity through retries. Treat migration, maintenance, backup, and reconciliation as parts of the database design.


## Correct these misconceptions
JPA does not remove SQL. A repository save is not necessarily a commit. MVCC does not eliminate locking. A cache read is not an inventory reservation. More connections do not guarantee more throughput. An index has write cost. A replica is not historical backup. A restore that starts successfully is not yet a verified business recovery.


## Retrieval assessment
Unanswered: Inventory commits a reservation, its response is lost, a replica serves stale availability, and the relay republishes the event after restart. Explain the constraints, query routing, retries, and receiving transaction needed to preserve stock and workflow correctness.


## Evidence and practice
Official sources were opened; the SQL function, concurrent-session lab, JPA mappings, migrations, and restore procedures were not executed. No measured plans or performance numbers are claimed. A later implementation must validate idempotency under concurrency, transaction rollback, SQL compatibility, migration locks, and restore/reconciliation on the selected environment.


Consult the [source ledger](chapters/06-postgresql-jpa/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-07"></a>

# Chapter7 revision — Kafka architecture

| Term | Precise meaning | Frequent mistake |
|---|---|---|
| Topic | Named stream divided into partitions | Assuming one global order |
| Partition | Ordered log with its own offsets | Treating a key as a uniqueness constraint |
| Offset | Logical position within a partition | Treating it as a byte address or order ID |
| Replica | Copy of partition data | Confusing it with a consumer group |
| ISR | Current in-sync membership | Assuming it always equals replication factor |
| High watermark | Replication commitment boundary | Confusing it with consumer progress |
| KRaft quorum | Metadata agreement | Assuming controllers store all user payloads |
| Compaction | Eventual removal of superseded keyed values | Expecting immediate uniqueness |
| Tombstone | Null-valued deletion marker under compaction semantics | Dropping it before a projection sees deletion |

Trace: metadata discovery → partition leader → batch append → follower replication → visibility under acknowledgment/isolation rules → consumer processing → independent checkpoint.

Storage estimate: ingress bytes/sec × retention seconds × replication, adjusted for measured compression, indexes, internal data, growth, and headroom. Recovery needs spare disk/network too.

Failure traps: unreachable advertised addresses; full disk; retention shorter than recovery; replicas sharing one failure domain; controller quorum healthy while a data partition is unavailable. Eligible leader replicas require feature-aware reasoning in Kafka4.2; do not use an outdated ISR-only election slogan.

Retrieval questions:
1. Draw three partitions with independent leaders and three replicas each.
2. Explain why a consumer can fall behind retention despite successful producer acknowledgments.
3. Contrast an append-only order history with a compacted current-order view.
4. Explain logical offsets, sparse indexes, segments, and page cache.
5. Predict one-zone loss with healthy controllers but insufficient data replicas.

Mastery evidence: [ ] explain without notes [ ] draw [ ] inspect metadata [ ] run a multi-broker failure lab [ ] verify business event identities [ ] answer assessment. No runtime evidence or mastery is recorded.



---

<a id="chapter-08"></a>

# Chapter 8 revision

- Enqueue != acknowledgment != business completion.
- Partition key groups related work; eventId identifies an event.
- Application resend is different from producer protocol retry.
- Buffer, request, and delivery timeouts cover different waits.
- acks=all works with ISR/minimum ISR; it does not wait for consumers.
- A stable outbox event plus business uniqueness survives ambiguous publication.

Retrieval: Draw the seven-step send path. Explain a lost acknowledgment. Calculate backlog storage for your own workload. Diagnose buffer exhaustion using metrics.

Mastery checklist: [ ] explain [ ] draw [ ] run demo [ ] inject failed response [ ] defend key [ ] compare settings [ ] answer assessment. No mastery recorded yet.



---

<a id="chapter-09"></a>

# Chapter 9 revision

| Concept | Meaning |
|---|---|
| Position | Next offset to be delivered by poll; distinct from internal prefetch |
| Committed offset | Recovery checkpoint |
| Group | Independent logical subscriber |
| Rebalance | Ownership change |
| Poll limit | Application progress bound |

Trace commit-before-work loss and work-before-commit duplicates. Solve out-of-order worker completion before adding threads. Diagnose per-partition lag and downstream waits.

Checklist: [ ] explain [ ] draw [ ] run two consumers [ ] prove replay safety [ ] solve worker exercise [ ] assessment. No lab execution or mastery recorded.



---

<a id="chapter-10"></a>

# Chapter10 revision

- State the scope of every delivery guarantee.
- Producer idempotence, Kafka transactions, and business idempotency solve different problems.
- read_committed filters aborted/uncommitted transactional data; it does not deduplicate business duplicates.
- Put input progress and Kafka output in the same Kafka transaction when implementing atomic transformations.
- PostgreSQL and HTTP effects require their own recovery design.

Retrieve: explain fencing, stable-offset visibility, partition-key migration, and a lost commit response. Draw the external-effect failure window.

Checklist: [ ] mechanism [ ] diagram [ ] failure exercise [ ] transaction lab [ ] assessment. Execution pending.



---

<a id="chapter-11"></a>

# Chapter11 revision

Factory creates clients; template publishes; container polls; listener adapts; service owns business changes. Decode failure can precede listener invocation. Failed work must not return as success. Retry topics change ordering. DLT needs ownership and confirmed publication.

Retrieve: draw decode and business error paths; classify stock rejection versus database outage; design a DLT replay approval/check process appropriate to the application.

Checklist: [ ] configure [ ] classify [ ] integration test [ ] crash/replay [ ] assess. All embedded examples unexecuted.



---

<a id="chapter-12"></a>

# Chapter12 revision

| Mechanism | Protects |
|---|---|
| Outbox | Durable publication intent |
| Inbox | Repeated event identity |
| Business unique key | Repeated business intention |
| Saga state | Multi-step durable progress |
| Reconciliation | Unknown external outcomes |

Retrieve: prove both dual-write gaps; draw relay duplicate window; trace late payment during cancellation. Invariants: no negative inventory, one reservation operation, explicit uncertain payment, compensation tracked.

Checklist: [ ] explain [ ] SQL local atomicity [ ] concurrent request test [ ] relay crash test [ ] Saga assessment. Labs unexecuted.



---

<a id="chapter-13"></a>

# Chapter13 revision

Backward: new reader, old data. Forward: old reader, new data. Full: both. Transitive: relevant older versions too. Validate syntax and meaning separately. Preserve Protobuf field numbers. Distinguish event identity, schema version, and aggregate version.

Practice: build a writer/reader matrix; evolve money fields; explain unknown enum behavior; prove replay compatibility.

Checklist: [ ] terms [ ] fixtures [ ] semantic assertions [ ] rollback plan [ ] assessment. Examples unexecuted.



---

<a id="chapter-14"></a>

# Chapter14 revision

Connect: worker/connector/task/converter/offset. Streams: topology/task/key/serde/state/changelog. Stream=events; table=current keyed view. Event time differs from processing time. Repartition redistributes by key. Changelog restores state. Sink effects need destination-specific guarantees.

Retrieve: solve event counts versus current counts; draw state restore; explain late events and join windows.

Checklist: [ ] concepts [ ] topology test [ ] real restart [ ] connector failure [ ] assessment. Execution pending.



---

<a id="chapter-15"></a>

# Chapter15 revision

Durability, availability, and recoverability are separate. RPO concerns acceptable data loss; RTO concerns service restoration. Diagnose per partition and end-to-end. Reassignment consumes capacity. TLS/authentication/ACLs solve different security questions. Mirroring is asynchronous unless a specific design proves otherwise.

Retrieve: calculate retention storage; diagnose hot-key lag; draw region promotion with fencing and reconciliation.

Checklist: [ ] metrics [ ] capacity [ ] read-only inspection [ ] multi-broker drill [ ] DR evidence [ ] assessment. Labs unexecuted.



---

<a id="chapter-16"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/16-mongodb/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Model around access and ownership, keep documents bounded, enforce conditional updates atomically, and design indexes from queries. Separate concerns from node selection. Make projection freshness, rebuilds, and version domains explicit.


## Correct these misconceptions
Document storage is not schema-free design. Embedding every event is not bounded modeling. Majority acknowledgment does not make every secondary read current. Sharding does not repair every slow query. A projection is not the owner of its copied business fact. Search freshness and database commit freshness can differ.


## Retrieval assessment
Unanswered: a full Order snapshot at version twelve arrives before version eleven, then Reporting loses its collection and only the last day's events remain. Explain normal processing, recovery prerequisites, and why a full rebuild cannot be promised from incomplete history alone.


## Evidence and practice
Sources and links were checked; no MongoDB deployment, mongosh command, Java adapter, index build, failover, or search experiment was executed. The example is scoped to an initialized document. Production initialization races, server version support, transaction settings, and projection rebuild correctness remain implementation validation tasks.


Consult the [source ledger](chapters/16-mongodb/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-17"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/17-redis/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Use Redis where copied or temporary state has explicit semantics. Plan misses, invalidation races, expiry, eviction, and cold recovery. Separate owner tokens from fencing and cache availability from business authority. Measure the load that escapes the cache.


## Correct these misconceptions
Cache memory is not free or infinite. TTL is not a precise durable timer. Lua atomicity does not span PostgreSQL. A lock key does not stop a paused old worker from writing elsewhere. Replication is not a historical backup. A high hit ratio does not prove safe outage capacity. Redis availability does not authorize stock changes.


## Retrieval assessment
Unanswered: a cache refresh lease expires during a JVM pause, a second worker refreshes newer data, and Redis then fails over. Describe the safe responsibilities of each worker and the owner database. Explain which errors are acceptable duplicate work and which would violate a business invariant.


## Evidence and practice
Official pages were opened; Redis commands, Lua, failover, persistence restore, and Spring adapters were not executed. No server/client release was installed or selected. Expected return values depend on completing the lease steps before expiry. The example verifies only a local key protocol if run, not distributed lease safety or authoritative commerce correctness.


Consult the [source ledger](chapters/17-redis/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-18"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/18-microservices-ddd/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Use business meaning and invariants to choose boundaries. Keep modules coherent before distributing them. Separate command models, read models, and authoritative histories. Make workflow ownership, late-event handling, and compensating obligations durable and explicit.


## Correct these misconceptions
DDD is not a requirement to create many services. A bounded context is not always one container. An aggregate is not an entire object graph. Sharing a database server does not imply shared write ownership. CQRS is not event sourcing. Publishing events is not proof of reconstructible history. Compensation is not global rollback.


## Retrieval assessment
Unanswered: Reporting shows READY while Order is CANCELLING, Inventory has reserved stock, and Payment's authorization result is unknown. Identify authoritative owners, valid customer communication, required reconciliation, and why a dashboard join cannot resolve the workflow by itself.


## Evidence and practice
Sources were checked; the Java program, module checks, state persistence, migration, and saga were not executed. Expected output is a prediction. The example covers only readiness evidence. Production cancellation, concurrent updates, event authenticity, durable deduplication, and reconciliation remain separate implementation verification tasks.


Consult the [source ledger](chapters/18-microservices-ddd/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-19"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/19-networking-gateway/notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Trace the entire path and label every timing, trust, and resource boundary. Reuse connections deliberately, budget deadlines, sanitize proxy metadata, and make retries depend on business safety. Gateways manage traffic policy while services retain domain ownership.


## Correct these misconceptions
DNS does not guarantee health. TCP delivery is not database commit. TLS does not replace resource authorization. A connection pool is not unlimited capacity. HTTP/2 does not remove every queue. A gateway timeout does not prove backend rollback. Discovery is not instant. Rate limiting and concurrency limiting solve different constraints.


## Retrieval assessment
Unanswered: a gateway times out at two seconds, Order commits at three seconds, the client retries through a different instance, and discovery still includes an old target. Explain the correctness mechanism and the independent network/operational fixes. Identify the evidence needed before changing timeouts.


## Evidence and practice
Sources were opened and version banners checked. The Java example, source launcher, socket lifecycle, Gateway route configuration, TLS, DNS, failover, and load behavior were not executed. No installed Spring Cloud compatibility or performance result is claimed. A later lab should verify stage timing, connection reuse, drain, retry identity, and limiter failure behavior on the actual deployment.


Consult the [source ledger](chapters/19-networking-gateway/source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-20"></a>

# Chapter20 revision — Docker

| Boundary | Remember |
|---|---|
| Image/container | Packaged content versus execution/writable layer |
| Host/container network | localhost belongs to the current namespace |
| EXPOSE/publish | Metadata versus runtime port mapping |
| Volume/backup | Persistent attachment versus independently recoverable copy |
| Heap/process memory | Heap excludes native buffers, stacks, metaspace and other overhead |
| Tag/digest | Movable name versus content identity |
| Startup/availability | Initial dependency order does not solve later outages |

Build: keep secrets out of context/layers; use secret mounts for build authentication; pin reviewed inputs; separate builder tools from runtime; run as an appropriate unprivileged user.

Run: externalize endpoints, preserve database state, make advertised Kafka addresses reachable, allow writable scratch paths, and align graceful shutdown with the container grace period.

Diagnose: capture exit reason, logs, CPU throttling, resident memory, disk state and dependency evidence before recreating everything. A kernel kill can occur before a Java heap error.

Worked recall: a512MiB container with512MiB heap has no room budgeted for non-heap memory. A PostgreSQL container with no durable volume can lose its data on replacement. An Order datasource pointing to localhost usually points at Order itself.

Mastery evidence: [ ] explain layers [ ] trace network paths [ ] review Dockerfile [ ] verify non-root execution [ ] preserve an order across replacement [ ] measure resources [ ] answer assessment. Infrastructure examples remain unexecuted.



---

<a id="chapter-21"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/21-kubernetes/notes.md); read its mechanisms, examples, ten explained interview answers, and solved exercises.

## Core model
Kubernetes reconciles infrastructure objects; applications preserve business obligations. Choose controllers by lifecycle, probes by decision, resources by evidence, and storage by recovery requirements. Review rendered deployment changes and test failure behavior across both platform and business boundaries.


## Correct these misconceptions
Running is not the same as ready. Readiness is not completed business work. A StatefulSet is not a database backup. A namespace alone does not deny network traffic. A memory limit is not an Xmx setting. HPA does not create unlimited database capacity. Helm does not reverse irreversible data changes. Restarting a Pod does not deduplicate a payment.


## Retrieval assessment
Unanswered: a rollout creates three new Pods that pass liveness but fail readiness, old Pods approach memory limits, and the database is at its connection ceiling. Explain the control-plane state, immediate recovery choices, and the capacity/schema evidence needed before proceeding.


## Evidence and practice
Official sources and version banners were checked. The YAML was not parsed by kubectl, applied, rendered through Helm, or exercised against an image. No cluster exists by implication. Future verification must include schema validation on the selected version, readiness, selector routing, rollout capacity, shutdown timing, OOM behavior, and outbox/inbox recovery under forced termination.


Consult the [source ledger](chapters/21-kubernetes/source-ledger.md) before version-specific claims. Draw failure boundaries and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-22"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/22-cloud-terraform/notes.md); read its mechanisms, examples, ten explained interview answers, and solved exercises.

## Core model
Choose cloud services from required guarantees and operating responsibility. Design identity and network paths explicitly, protect infrastructure state, and review concrete changes. Model cost in units and validate recovery against measured business outcomes, not only resource availability.


## Correct these misconceptions
Managed means fewer selected operational duties, not zero responsibility. Multi-zone is not universal regional disaster recovery. A private address does not replace authorization. A replica is not historical backup. Terraform plan is not a business-safety proof. Sensitive output marking is not state encryption. Budget alerts are not necessarily spending caps.


## Retrieval assessment
Unanswered: a regional recovery restores infrastructure quickly but lacks recent Order records, while Payment retains newer authorizations and Terraform state contains an old resource binding. Explain the independent infrastructure, state-management, and business reconciliation tasks before admitting new checkout traffic.


## Evidence and practice
Official sources were checked; Terraform was not installed or run, HCL was not parsed, and no cloud resources, credentials, paid actions, pricing estimates, restores, or failovers were executed. Arithmetic is illustrative. Future validation must select versions/regions, review concrete plans, verify network and identity paths, and measure restoration plus business reconciliation.


Consult the [source ledger](chapters/22-cloud-terraform/source-ledger.md) before version-specific claims. Draw failure boundaries and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-23"></a>

# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](chapters/23-security/notes.md); read its mechanisms, examples, ten explained interview answers, and solved exercises.

## Core model
Validate identity evidence in its issuer, audience, purpose, and time context. Enforce action/resource/tenant policy at the owning service. Protect browser and input boundaries, manage secrets throughout their lifecycle, and verify threat mitigations with explicit negative cases and safe failure behavior.


## Correct these misconceptions
A valid JWT is not universal authorization. Signed does not mean encrypted. An ID token is not an API access token. CORS is not a firewall. Stateless does not automatically mean CSRF-safe. mTLS does not replace business policy. A secret manager does not justify broad access. A successful happy-path login is not a security test suite.


## Retrieval assessment
Unanswered: an authenticated support agent requests another tenant's export, a service presents a valid certificate but the wrong token audience, and key discovery is temporarily unavailable. Explain the independent decisions, safe responses, audit evidence, and recovery policy for each case.


## Evidence and practice
Official references were opened; no Java execution, identity-provider flow, token validation, certificate rotation, Spring context, scanner, or penetration test was run. Expected output is predicted. OWASP Top 10 provides risk awareness and ASVS supports verification requirements; mentioning them does not establish compliance. A later implementation must test authentication negatives, object/tenant authorization, browser flows, key rotation, secrets, and safe failure behavior.


Consult the [source ledger](chapters/23-security/source-ledger.md) before version-specific claims. Draw trust and failure boundaries, then identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.



---

<a id="chapter-24"></a>

# Chapter24 revision

| Signal | Question |
|---|---|
| Metric | How often/how much/how long? |
| Log | What happened to this operation? |
| Trace | Where did this causal path spend time? |
| Profile | What code/resource consumed runtime? |

SLI=measurement; SLO=target/window; error budget=permitted bad outcomes. Aggregate buckets before fleet quantiles. Keep IDs out of metric labels. Measure completion separately from acceptance.

Practice: define an order-completion SLI, diagnose healthy HTTP with stuck payments, and calculate a10x budget burn.

Checklist: [ ] instrumentation [ ] context propagation [ ] cardinality [ ] SLI [ ] diagnosis [ ] assessment. Examples unexecuted.



---

<a id="chapter-25"></a>

# Chapter25 revision

Unit=logic; integration=real boundary; contract=shared assumptions; E2E=journey; failure=interrupted recovery. Mocks cannot prove database locks. Container configuration limits evidence. JUnit6 retains Jupiter packages. Maven verify runs only configured checks; inspect reports. Promote one artifact. Expand/contract enables safer schema evolution.

Practice: test concurrent idempotent requests, fix an asynchronous flaky test, and design a rollback after schema expansion.

Checklist: [ ] test levels [ ] real DB [ ] Kafka crash boundary [ ] contract matrix [ ] reports [ ] release evidence. Examples unexecuted.



---

<a id="chapter-26"></a>

# Chapter26 revision

Safety: forbidden outcomes never occur. Liveness: useful work progresses under assumptions. Linearizability respects real-time operation order; serializability concerns transaction equivalence. CAP concerns partitions; PACELC includes healthy-operation latency. Quorum intersection is only one proof ingredient. Fencing rejects obsolete ownership at the resource.

Practice: draw a lost-response timeline; calculate nested retry multiplication; solve regional stock allocation; explain lease expiry after a pause.

Checklist: [ ] models [ ] assumptions [ ] fencing simulation [ ] retry budget [ ] failure reasoning [ ] assessment. No distributed protocol execution claimed.



---

<a id="chapter-27"></a>

# Chapter27 revision

Throughput=useful completions/time; latency=service+waiting. Little's Law: L=lambda×W with matching stable-system boundaries. Backlog drain=backlog/(capacity-arrivals) when the difference is positive. Pools bound concurrency; queues defer work. Open/closed load models expose different behavior. JMH addresses JVM measurement pitfalls; JFR helps find runtime cost.

Practice: calculate queue fill/drain; diagnose low CPU with high latency; compare allocation with retention; write a complete benchmark report without invented results.

Checklist: [ ] estimates [ ] profiling [ ] real load model [ ] correctness checks [ ] before/after evidence [ ] assessment. Examples unexecuted.



---

<a id="chapter-28"></a>

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



---

<a id="chapter-29"></a>

# Chapter 29 revision

- Encapsulation protects invariants, not just fields.
- Identity and value equality answer different questions.
- Interfaces describe real capabilities and variation.
- State governs lifecycle; Strategy selects policy.
- An Adapter translates semantics, not merely JSON.
- In-process Observer is not durable publication.
- JVM synchronization does not coordinate replicas.
- Errors must preserve information needed for recovery.

Practice the Order cancellation example, then add PENDING payment. Draw classes and a state machine before adding frameworks. Run legal/illegal/duplicate/late transition tests after implementation.

Mastery evidence: implementation, behavior tests, explained concurrency boundary, and a demonstrated change scenario. Material available; proficiency unassessed.



---

<a id="chapter-30"></a>

# Chapter 30 revision

Sequence: baseline -> hypothesis -> precise fault -> evidence -> restore -> reconcile -> invariant checks -> report.

Know the difference: fault/symptom/cause; mitigation/fix; restart/recovery; message duplicate/business duplicate; prediction/observation.

Five critical boundaries: before DB commit, after commit before publish, after publish before relay marker, after consumer DB commit before offset, after external effect before response.

Practice the 25 labs only in their required isolated environments. Single-broker downtime is not replicated leader failover. Process stop is not every network partition. Never reset offsets or delete history merely to clear a dashboard.

Evidence: operation IDs, state queries, telemetry, exact fault, restored invariants. All labs are procedures, not executed results.



---

<a id="chapter-31"></a>

# Chapter31 revision

Answer sequence: constraint -> direct answer -> mechanism -> boundary -> failure -> alternative -> verification.

Evidence categories: professional work / executed project lab / conceptual knowledge. Never merge them.

Mock score: correctness, mechanism, failure, trade-offs, clarity/evidence; each0–2. Ask one question and wait. A model answer is study material, not independent performance.

Portfolio: reproducible build, actual tests, contracts, migrations, outbox/Saga/idempotency evidence, deployment status, observability/security, ADRs, runbooks, measured performance, limitations.

Practice a two-minute payment-timeout answer and a five-minute architecture trace. Use only truthful examples for leadership, incidents, mentoring, conflict, and delivery.

