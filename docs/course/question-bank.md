# Study questions and model answers

Ten study questions per chapter. Each full chapter also has a separate unanswered assessment. No learner score is inferred.

# Chapter 1 — Foundations of Distributed Systems: Java 21, Spring Boot, REST APIs, Messaging, and Why Apache Kafka Exists


### Q1. What makes partial failure difficult? [P0]
Fundamental: some components can succeed while others fail or cannot observe success.
Senior answer: identify each effect, acknowledgment, and recovery boundary. A caller has incomplete knowledge of remote state.
Example: payment committed but response lost.
Failure/trade-off: blindly retrying can duplicate an effect; never retrying can leave work unresolved.
Common mistake: equating timeout with business rejection.
Follow-up: how would you recover after both services restart?
Reference: [H1]; the payment sequence is our worked example.

### Q2. When would you keep a modular monolith? [P0]
Fundamental: when one deployment and local coordination meet the needs.
Senior answer: assess team ownership, change coupling, independent scaling, data invariants, and operational capacity before adding boundaries.
Example: a small order system with one team and shared transactional needs.
Trade-off: less deployment independence, but fewer network failure paths.
Common mistake: claiming microservices automatically scale better.
Follow-up: which evidence would justify extracting Inventory?
Reference: architectural recommendation; no universal official rule determines service boundaries.

### Q3. How is asynchronous communication different from an asynchronous Java method? [P0]
Fundamental: a future changes waiting behavior; durable messaging changes how work survives and is delivered.
Senior answer: separate thread scheduling, transport acknowledgment, and business completion.
Example: returning CompletableFuture does not persist a payment request.
Failure: the process can stop while work exists only in memory.
Common mistake: treating async as durable.
Follow-up: what exactly does successful producer completion establish?
Reference: [K5], [V1].

### Q4. How do you compare REST, JMS, and Kafka fairly? [P0]
Fundamental: they are different categories: architectural style, Java messaging specification, and streaming platform.
Senior answer: compare concrete implementations and required contracts, not category labels alone.
Example: HTTP for order lookup; Kafka for independently consumed order changes; JMS for an established enterprise queue integration.
Trade-off: replay/retention needs differ from single-consumer work distribution.
Common mistake: saying JMS has no topics.
Follow-up: can HTTP accept asynchronous work?
Reference: [H1], [J1], [K1].

### Q5. Why retain events after consumption? [P0]
Fundamental: another reader or later rebuild may still need them.
Senior answer: retention enables independent progress, but replay must account for contract evolution and side effects.
Example: rebuilding a reporting projection without triggering fresh payment operations.
Trade-off: storage and privacy/lifecycle responsibilities grow with retained history.
Common mistake: assuming retention means unlimited history.
Follow-up: what if the required data has already expired?
Reference: [K6].

### Q6. What does a partition key solve? [P0]
Fundamental: it helps select a partition and group related records.
Senior answer: key selection balances locality/ordering requirements against skew; stable routing assumptions matter.
Example: orderId groups order-related records under an unchanged partitioning scheme.
Failure: adding partitions or using different partitioners can change routing; downstream parallel work can reorder effects.
Common mistake: claiming global business order from a shared key.
Follow-up: why does orderId not prevent inventory overselling across orders?
Reference: [K1], [K4]; inventory conclusion is our project reasoning.

### Q7. Does adding consumers always improve throughput? [P0]
Fundamental: traditional group partition ownership bounds useful parallelism.
Senior answer: partition skew, processing cost, downstream pools, and rebalances can be limiting factors.
Example: an additional member can be idle when every partition already has an owner.
Trade-off: more processing can overload PostgreSQL.
Common mistake: treating consumer count as a universal throughput multiplier.
Follow-up: which metric separates a hot partition from database saturation?
Reference: [K4].

### Q8. Does producer acknowledgment mean an order completed? [P0]
Fundamental: no; it concerns publication under configured semantics.
Senior answer: downstream consumption, database changes, and business transitions require their own evidence.
Example: reporting can be hours behind an accepted event.
Trade-off: decoupling requires status visibility and backlog management.
Common mistake: returning 'paid' when only a command was published.
Follow-up: what should the API return after durable order creation?
Reference: [K5], [H1].

### Q9. Why does an outbox still require idempotent consumers? [P0]
Fundamental: the relay can publish and crash before recording publication.
Senior answer: retry bridges a failure gap but can repeat delivery; couple consumer deduplication with its business mutation and outgoing records.
Example: inventory must not reserve twice on republished commands.
Trade-off: deduplication storage and retention/replay policy need ownership.
Common mistake: describing outbox as end-to-end exactly-once delivery.
Follow-up: what if two different event IDs request the same business operation?
Reference: [K2], [S2]; outbox protocol is our architectural design.

### Q10. Why does @Transactional not undo an HTTP payment? [P0]
Fundamental: the normal database transaction does not control the remote provider's effect.
Senior answer: enumerate transaction managers and resources; distinguish synchronization from atomic commit and use compensations/reconciliation for external effects.
Example: local order rollback after a remote authorization.
Trade-off: compensations can fail and need durable tracking.
Common mistake: assuming one annotation spans every call inside the method.
Follow-up: what evidence is needed before declaring cancellation complete?
Reference: [S1], [S2].



---

# Chapter 2 — Java 21 and the JVM: From Domain Types to Production Evidence


1. **Does a final reference make an object immutable?** No. It prevents reassignment of that reference after initialization under its applicable rules; a referenced list can still change. Our request constructor copies the list, and its elements are immutable values. Explain the entire reachable graph when claiming immutability.

2. **Why use a record for a request?** It expresses a transparent value carrier with generated component behavior. Validation and copying remain our responsibility. A request is a better fit than an entity whose identity and lifecycle require specialized framework behavior.

3. **Why can a Java process exceed its heap size?** Heap is only one memory category. Native allocations, thread resources, metadata, and generated code contribute. Compare process memory with category evidence rather than raising Xmx automatically.

4. **How can Java leak memory with GC?** An unwanted object can remain reachable. The collector cannot infer that yesterday's requests should have been removed from a static collection. Investigate retaining paths and lifecycle ownership, then verify a stable live set.

5. **Why can startup traffic be slower?** Class loading, initialization, connection establishment, cache population, and compilation can occur early. Measure these separately. A health response does not prove representative business paths are warm.

6. **What does a class-loading error suggest?** Missing or incompatible artifacts are plausible; static initialization is a separate stage. Identify the exact error and dependency/version context. Retrying requests will not create a missing class.

7. **Is a G1 pause target a deadline guarantee?** No. A target guides a heuristic collector under workload constraints. A strict latency requirement needs measured tails, headroom, and application-level overload behavior, not one VM flag.

8. **When do you choose a thread dump over a heap dump?** Start with threads when requests appear blocked or stuck; inspect heap ownership when memory grows. Both are snapshots, while a recording provides temporal context. Select evidence from the question being asked.

9. **Why is one stopwatch invocation a weak benchmark?** Startup and optimization can dominate, inputs may be unrealistic, and unused work can be optimized. A controlled benchmark requires warm-up, useful results, representative state, and statistical interpretation.

10. **Can sealed types solve event schema evolution?** They help local exhaustiveness. Remote producers and consumers deploy independently, so unknown wire variants require versioned contracts and explicit handling. Compiler completeness within one artifact does not prove distributed compatibility.



---

# Chapter 3 — Concurrency, the Java Memory Model, and Bounded Work


1. **Why does volatile not make decrement safe?** Visibility and ordering do not combine a read, decision, and write into one protected transition. Two buyers can both see stock and both accept. Use an atomic conditional update or a lock over the invariant.

2. **What is happens-before?** It is a specified ordering relation that constrains visibility, not a wall-clock observation. Identify an actual synchronization edge, such as monitor unlock/lock or task completion observation. Sleeping is not such a protocol.

3. **When use a lock instead of AtomicInteger?** Use a lock when several state components must change coherently or when the transition is easier to reason about under exclusion. A single atomic counter cannot atomically maintain a separate reservation map and an external effect.

4. **Is ConcurrentHashMap enough for idempotency?** It may support an atomic local key operation, but restart and other replicas remain. Durable idempotency requires persisted operation identity and business constraints. A map is useful for local coordination, not the entire protocol.

5. **Why can maximumPoolSize be misleading?** An unbounded queue can absorb submissions before additional workers are needed under the executor's policy. Inspect queue type and rejection behavior alongside thread limits. Bounded active threads do not imply bounded retained work.

6. **Does a future timeout stop payment?** It may only stop waiting or complete the future exceptionally. The effect could still happen. Use client deadlines, cooperative cancellation where supported, stable operation identity, and reconciliation of unknown results.

7. **What do virtual threads improve?** They reduce the cost of many waiting tasks. CPU capacity, database connections, rate limits, and memory remain bounded. In Java 21, account for pinning and inspect actual behavior under the chosen libraries.

8. **What is backpressure?** A downstream capacity signal influences upstream work admission or production. A finite queue with a documented rejection or waiting policy is one component. An ever-growing queue delays failure rather than communicating capacity.

9. **Why avoid payment calls under a lock?** A slow remote call extends local exclusion and couples all contenders to network latency. It may also complete after a timeout. Persist intent and perform the remote step using a durable workflow boundary.

10. **How distinguish deadlock from saturation?** Deadlock has a cycle of dependencies that cannot progress under the current protocol. Saturation can have workers making progress while arrivals exceed service rate. Thread evidence plus throughput and queue age distinguishes them better than a single blocked-thread count.



---

# Chapter 4 — Spring Internals: Objects, Proxies, Requests, and Transactions


1. **What is IoC?** The container owns assembly and lifecycle instead of every class constructing its collaborators. Constructor injection makes that assembly contract explicit. It does not remove the need to design responsibilities or thread safety.

2. **What is a bean?** It is a container-managed object with configured creation and lifecycle. A bean can be an application service, client, or configuration component. It is not synonymous with a database row or a new object per request.

3. **How does auto-configuration work?** Conditions select configuration based on the environment and existing definitions. Inspect the condition report and dependency graph when behavior changes. A default is an input to diagnosis, not a substitute for checking effective configuration.

4. **Why can self-invocation bypass a transaction?** The target calls itself without returning through the exposed proxy. The advice therefore has no interception opportunity. Refactor use-case boundaries or choose another explicit transaction mechanism when appropriate.

5. **Does catching an exception guarantee commit?** No. A participating transaction may already be rollback-only, and persistence errors can affect resource state. Understand propagation and rollback policy; do not hide the exception and assume the database is clean.

6. **Why can REQUIRES_NEW exhaust a pool?** The outer transaction may retain its connection while the inner requests a different one. Many nested callers can consume all available capacity. Independent commit semantics need a genuine business reason and a capacity analysis.

7. **Does async inherit a transaction?** Do not assume thread-bound transaction resources move to another execution thread. Establish the intended transaction in the worker and persist any obligation that must survive a crash. An annotation is not a durable job queue.

8. **When choose MVC over WebFlux?** For a blocking JPA stack and straightforward request flow, MVC is often easier to reason about. WebFlux suits an end-to-end non-blocking composition with explicit cancellation and backpressure. Choose based on dependencies and requirements, not fashion.

9. **Why avoid returning an entity from a controller?** Serialization may trigger database access, expose internal fields, or tie the public API to persistence changes. An explicit response value bounds data selection and makes compatibility intentional.

10. **What does graceful shutdown guarantee?** It provides a configured opportunity to stop and drain under supported termination conditions. It cannot guarantee completion after a hard kill or arbitrary infrastructure failure. Durable work and idempotent recovery remain necessary.



---

# Chapter 5 — APIs and Integration Contracts


1. **Does 201 mean an order is paid?** It means the resource was created under the API contract. Business state can remain pending. Clients need a status model and cannot infer fulfillment readiness from transport success.

2. **Can POST be retried safely?** Only when the operation contract makes retries safe, for example through durable idempotency identity and payload matching. Method choice alone does not enforce the business invariant after response loss.

3. **What does OpenAPI prove?** It describes an HTTP interface and enables useful tooling. It does not prove authorization correctness, atomic idempotency, or recovery. Behavioral tests must validate those properties separately.

4. **Why reserve removed Protobuf tags?** Old serialized data and independently deployed consumers may still contain them. Reusing a tag can reinterpret bytes with a new meaning. Preserve wire and business compatibility together.

5. **When choose gRPC?** Typed internal clients and streaming can justify it when the ecosystem supports the protocol. Include deadlines, observability, compatibility, and browser/gateway constraints in the decision instead of treating it as a local method call.

6. **Does GraphQL eliminate N+1 work?** No. It shapes client selection while resolvers can still fetch one item at a time. Batch and bound resolver work, enforce authorization, and measure downstream request count.

7. **SSE or WebSocket for status?** SSE fits primarily server-to-client updates with HTTP-based delivery and reconnect conventions. WebSocket supports bidirectional interaction but requires a richer session protocol. Both need recovery and resource limits.

8. **Why deduplicate a webhook?** A sender may redeliver or the acknowledgment may be lost. Persist delivery identity and apply the effect atomically. Also enforce business uniqueness because two different deliveries can describe the same logical operation.

9. **JMS versus AMQP?** JMS/Jakarta Messaging is a Java API specification, whereas AMQP specifies wire-level behavior. A provider may expose both, but their version and feature contracts are separate. Always identify the provider and protocol version.

10. **Why share a deadline across calls?** Independent full-length timeouts at every hop can exceed the caller's useful waiting budget. Allocate the remaining budget and stop useless work where possible. Reconcile effects that may already have committed.



---

# Chapter 6 — PostgreSQL, JPA, and Durable Business Invariants


1. **Does a transaction prevent overselling automatically?** No. The isolation level and statements must enforce the invariant. A conditional update, suitable locks, or a verified serializable design can protect it. An unlocked read followed by a write may still lose a concurrent decision.

2. **What does MVCC provide?** Snapshot-based visibility lets many reads avoid ordinary writer blocking. It does not eliminate writer conflicts, locks, or maintenance. Explain which snapshot a statement uses before claiming a stable view.

3. **Optimistic or pessimistic locking?** Optimistic version checks suit conflicts that are uncommon and retryable. Pessimistic locks can serialize conflicting access but create waits and deadlock considerations. Choose based on contention and invariant scope, then measure.

4. **Flush versus commit?** Flush synchronizes pending persistence changes with database operations; commit completes the transaction. Errors can surface at flush or commit. A successful save call is not necessarily durable completion.

5. **Why does N+1 happen?** A parent query is followed by per-parent association loads. The Java loop conceals repeated database round trips. Design a fetch plan or projection and verify actual SQL instead of making every relationship eager.

6. **Why might an index be ignored?** The query may return much of a small table, statistics may influence estimates, or predicate/order shape may not suit the index. Compare estimates with actual work. Index usage alone is not a performance objective.

7. **Why retry an entire serializable transaction?** Earlier reads and decisions belong to the failed attempt. Repeating only a final write can retain invalid assumptions. Keep the transaction's external effects out of the retry loop unless they are independently idempotent.

8. **Is a replica a backup?** It supports availability/read scaling but can replicate mistakes. Historical recovery requires retained backup material and a tested restore procedure. Verify business reconciliation as well as database startup.

9. **Partitioning versus sharding?** Partitioning organizes a logical table within the database design; sharding distributes ownership across database groups. Sharding introduces routing and cross-shard semantics. Neither is a free fix for an unindexed query.

10. **How does outbox help?** It atomically records local state and a publication obligation. A relay may publish duplicates, so downstream enforcement remains necessary. State the boundary rather than claiming one transaction spans every service.



---

# Chapter 7 — Kafka Architecture, Storage, and Replication


### Q1. Is an offset a byte position? [P0]
No. It is a logical partition position. The offset index maps selected logical offsets to file positions. If you treat offset differences as exact bytes or exact business-event counts, compaction and control records make your estimate misleading. Follow-up: explain how a timestamp lookup finds a starting point.

### Q2. Does consumption remove data? [P0]
Ordinary consumer progress is separate from retention. A second group can read the same retained log. The senior concern is whether the slow group can finish before expiration; acknowledgment by the fast group provides no protection.

### Q3. Why split a partition into segments? [P0]
Segments bound file management, cleanup, and recovery work. Our operational implication is that retention is not a precise per-record deletion timer. Follow-up: explain why a small topic might retain records beyond its nominal age threshold.

### Q4. How does KRaft differ from replication? [P0]
The controller quorum agrees on metadata, while partition replicas hold records. Losing controller availability and losing an order partition leader are distinct incidents. More controllers do not create more copies of order events.

### Q5. Is ISR the replication factor? [P0]
Replication factor is intended placement; ISR is current synchronization membership. A partition configured with three copies can have fewer synchronized replicas. Monitoring only configured replication factor misses degraded durability.

### Q6. What does high watermark mean? [P0]
It bounds replication-committed records, with transactional isolation potentially limiting visibility further. It does not mean a customer was charged or a consumer committed its offset. Name the boundary before using the word committed.

### Q7. Can compaction replace an audit log? [P0]
Usually not when intermediate transitions matter. Compaction supports latest-value reconstruction, while an audit trail needs retained occurrences. Our order history and reporting snapshot therefore have different topic policies.

### Q8. Why might bootstrap succeed but publication fail? [P0]
Metadata can advertise a broker address unreachable from the client's network. Inspect advertised listeners and DNS from the client environment. Repeatedly enlarging request timeouts does not fix an unreachable hostname.

### Q9. Are all non-ISR leader elections unsafe in 4.2? [P1]
No. Eligible leader replicas extend the safe selection model when the feature is enabled. An interview answer should distinguish ELR from arbitrary unclean election and inspect feature state in an upgraded cluster.

### Q10. Why is replica count insufficient for resilience? [P0]
Copies can share a failure domain. Three brokers on one storage device can fail together. Evaluate independent placement, spare capacity, and restoration procedures, then demonstrate the claimed tolerance with a controlled lab.



---

# Chapter 8 — Kafka producers: from a method call to a durable record

1. **Does send returning mean success?** No: it can mean the client accepted pending work. Observe its completion. In an order API, confusing those moments reports durability that has not been established.
2. **Why share a producer?** It owns expensive connections, buffers, and a sender; sharing improves utilization. It is designed for concurrent use, unlike KafkaConsumer. Share lifecycle deliberately rather than closing it from arbitrary requests.
3. **What does acks=all buy?** Stronger replication acknowledgment than leader-only acceptance under the ISR rules. It does not acknowledge consumer business actions, and its practical durability depends on replica configuration and health.
4. **Does idempotence stop duplicate orders?** It addresses the producer retry mechanism. A repeated HTTP request or regenerated event can be a new transport operation. Enforce business keys and unique constraints at the service owning the effect.
5. **Why can an asynchronous producer stall?** It still needs metadata, buffer capacity, and serialization. Investigate backpressure and callback work before increasing timeouts.
6. **Why can increasing partitions change behavior?** Key-to-partition mapping may change when the partition count changes. Historical and new records can sit in different partitions, so sequence continuity needs a migration plan.
7. **Should every event use a random key?** Only when the required semantics permit it. Random keys distribute load but do not preserve per-order grouping; eventId and partition key serve different purposes.
8. **Why not retry every exception?** Some failures are deterministic or indicate invalid ownership. Repeating malformed data wastes resources; a fenced transactional producer must stop acting as owner.
9. **Why is flush per record expensive?** It forces pending sends toward completion and reduces useful overlap. Flush at a justified lifecycle or checkpoint boundary, not as a substitute for understanding acknowledgment.
10. **How do you defend a tuning choice?** State workload, latency target, baseline metrics, changed variable, measured benefit, and failure impact. A configuration copied without these conditions is an untested hypothesis.



---

# Chapter 9 — Kafka consumers, offsets, groups, and rebalances

1. **Why do two services need different groups?** Each service requires its own view and checkpoint. Sharing a group divides records between them, so some orders may reach Inventory and others only Reporting.
2. **What does committing 42 mean?** Resume at the next eligible record from position 42. It is not a command to delete record 41. The checkpoint's meaning must match completed work.
3. **Does earliest always replay?** No. It applies when there is no usable committed position. A valid stored checkpoint takes precedence; a new group is a safer learning replay.
4. **Can one consumer be used by many worker threads?** Normal consumer operations are not thread-safe. Keep the consumer on one owner thread and exchange bounded work/results with workers.
5. **Why can duplicates happen after a successful DB commit?** The process may die before committing its Kafka checkpoint. On restart the old checkpoint causes redelivery. The duplicate is a recovery consequence, not necessarily broker corruption.
6. **Can more consumers always reduce lag?** Partition count, skew, and downstream limits constrain useful concurrency. Adding consumers to a saturated database can reduce total useful throughput.
7. **What is the danger of auto-commit?** Its timing may not match durable application work, especially with worker handoff. It is not inherently invalid, but the application must prove its relationship to completion.
8. **Why are heartbeat and poll limits separate?** Connectivity and application progress are different. A live process can remain stuck on a dependency, so membership needs both notions.
9. **Does cooperative assignment remove replay?** No. Crashes and checkpoint gaps remain. It changes partition movement behavior, not business idempotency.
10. **What would you inspect during a rebalance storm?** Deployment churn, processing stalls, GC pauses, membership/configuration changes, broker/coordinator health, and partition counts. Correlate timestamps instead of treating every rebalance as the root cause.



---

# Chapter 10 — Ordering, delivery guarantees, and Kafka transactions

1. **What does exactly once mean here?** A committed result within a stated boundary despite retries. Code can execute repeatedly. Name Kafka outputs and offsets explicitly; do not silently include external systems.
2. **Why is idempotence insufficient for two topics?** It protects retry behavior for publication, but a logical multi-record unit needs transaction commit/abort visibility. Otherwise one destination may contain the result while another does not.
3. **Does read_committed prevent all duplicates?** No. Two separately committed business duplicates remain two valid records. Business identity remains necessary.
4. **Why does a long transaction affect readers?** Visibility must not step past unresolved transactional work as if it were final. Investigate stable-offset blockage when data exists but committed readers appear stuck.
5. **Can a PostgreSQL commit join a Kafka transaction automatically?** No. Being in the same Java method does not create a common atomic resource. Use a proven cross-resource pattern such as the outbox for this project.
6. **What is fencing?** Preventing an obsolete owner from continuing writes after a newer owner is established. It protects against overlapping process lifetimes rather than merely checking whether a process is alive.
7. **Does one partition guarantee business order?** It orders stored records. Concurrent handlers and racing publishers can still produce unintended state transitions. Enforce sequence and state-machine rules where effects occur.
8. **Why commit offsets in the producer transaction?** To couple accepted output with input progress. Two independent commits leave either a duplicate-output or lost-output failure window.
9. **Is a committed transaction immediately seen by every consumer?** Consumers still poll at different times and may be behind. Atomic visibility is not simultaneous delivery to all observers.
10. **How would you test the guarantee?** Inject crashes at each boundary, consume with both isolation levels, and count committed output identities. Include an external-effect counterexample to show the scope honestly.



---

# Chapter 11 — Spring Kafka integration and recoverable listeners

1. **What creates the poll loop?** The listener container. The annotation identifies an endpoint and adapter; it does not magically implement domain reliability.
2. **Why separate a listener from a service?** Transport handling and business invariants change for different reasons. A transactional domain service can be tested independently while the adapter is tested against Kafka.
3. **Why can malformed JSON bypass my catch block?** Deserialization occurs before method invocation. Configure a deserialization error path at the client/container boundary.
4. **Is manual acknowledgment the safest choice?** Only with a proven timing model. A premature manual call loses work just as a premature automatic checkpoint can.
5. **Should insufficient stock go to DLT?** Usually no. It is an expected business outcome with an event and state transition. DLT is for work that cannot be interpreted or completed under the normal contract.
6. **What changes with retry topics?** Work is deferred into another topic and processing order may change. This improves isolation for some workloads but requires explicit order/version handling.
7. **When is a DLT handoff complete?** When publication is durably confirmed under the chosen policy. Logging a send request is insufficient evidence.
8. **Why not use arbitrary concurrency?** Partitions bound assignment and dependencies bound useful parallelism. More threads can increase contention and retries.
9. **What must an integration test prove?** Real serialization, container behavior, checkpoint recovery, and the business database invariant. A mocked listener invocation only proves local method behavior.
10. **How do transactions affect error handling?** Exceptions determine rollback and recovery paths. Swallowing one can commit unintended progress; use the transaction-specific documented mechanism.



---

# Chapter 12 — Outbox, inbox, business idempotency, and Saga orchestration

1. **Why can't write order reverse fix dual writes?** Either order leaves a crash point between independently committed resources. The outbox changes the atomic boundary by colocating state and intent.
2. **Does the outbox eliminate duplicates?** No. Acknowledgment can precede relay bookkeeping. Repeated publication is expected and consumers must make it harmless.
3. **Why both inbox and business uniqueness?** Inbox catches replay of one event; business uniqueness catches different events representing one operation. They protect different identities.
4. **Is compensation rollback?** No. It is a new action against current reality and may itself fail. A refund preserves the history of a prior charge.
5. **Why retain UNKNOWN payment state?** A timeout is insufficient evidence of failure. Reconciliation avoids both double charges and incorrectly cancelling paid orders.
6. **Does CDC guarantee business order automatically?** It exposes committed change history under connector semantics. Mapping, partition keys, multiple transactions, and aggregate versions still need a designed contract.
7. **Can Redis prevent overselling here?** It can assist caching or admission, but PostgreSQL owns authoritative inventory and its invariant. A cache or lease must not become an unproven second authority.
8. **What happens if compensation fails?** Persist the failure, retry within policy, alert on age, and provide reconciliation/manual resolution. Do not label the Saga cancelled while resources remain reserved.
9. **Why use an orchestrator?** It makes this multi-step lifecycle explicit and inspectable. The trade-off is coordinating logic and availability responsibility in the Order context.
10. **How do you prove correctness?** Enumerate crash boundaries and assert invariants: one accepted request identity, one reservation operation, no negative stock, one payment intention, and eventual resolution or visible unresolved state.



---

# Chapter 13 — Event schemas, compatibility, and independent deployments

1. **Is JSON schemaless?** It has syntax and the application still assumes a structure. Without explicit validation, the contract is implicit and failures move into runtime code.
2. **What is backward compatibility?** A new reader can interpret older writer data under the chosen rules. Test old fixtures against new readers, including historical versions.
3. **Why is forward compatibility useful?** Rolling deployments and rollback leave old readers alive while new writers publish. New data must not break them unless the rollout deliberately prevents overlap.
4. **Does adding an optional field always work?** Not if consumers reject unknown fields or generated mappings impose stricter assumptions. Wire, decoder, and business compatibility all matter.
5. **Why reserve Protobuf field numbers?** A reused number can reinterpret persisted bytes as unrelated data. The identity survives source-code renaming.
6. **Does a registry validate business correctness?** It can validate configured structural evolution, not whether money units or event meaning changed incorrectly. Business fixtures and review remain necessary.
7. **Why avoid entity serialization?** Persistence details, lazy relationships, and internal fields leak into a public contract. Independent deployment then becomes coupled to database implementation.
8. **What does transitive compatibility add?** It checks a longer version history. This matters when replay spans several releases, not only yesterday's messages.
9. **Should every breaking change create a topic?** Sometimes, but not automatically. Evaluate parallel consumers, migration traffic, retention, and operational cost. A new topic is a boundary, not a migration plan.
10. **What is the strongest evolution test?** A writer/reader matrix with historical fixtures plus business assertions, including unknown fields, missing fields, enum changes, and semantic units.



---

# Chapter 14 — Kafka Connect and Kafka Streams

1. **Connect or Streams?** Connect moves data through reusable integrations; Streams computes on Kafka data inside an application. Many systems use both with separate ownership.
2. **Connector versus task?** A connector plans/configures transfer; tasks execute units of transfer. Increasing tasks.max cannot invent source parallelism a connector does not support.
3. **Why do source offsets differ from Kafka offsets?** They describe another system's progress. Recovery must relate the external position to emitted data, not confuse two coordinate systems.
4. **KStream versus KTable?** One models event occurrences, the other keyed evolving state. Repeated status updates affect their aggregates differently.
5. **Why repartition?** A key change can require data redistribution so all values for a new key reach the correct task. That adds topics, I/O, and serialization boundaries.
6. **What restores local state?** Changelog data and replay under the topology's semantics. Persistent disks and standby replicas can reduce delay but require their own operation.
7. **What does grace solve?** It defines tolerated event-time lateness for window processing. It cannot make arbitrarily late data free or infer missing business history.
8. **Does Streams EOS cover a sink database?** Not automatically. The sink connector and destination must provide their own effect/progress guarantee.
9. **Why test a topology without Kafka?** A topology test driver makes transformations and time cases deterministic. It does not prove broker failover, network behavior, or distributed restoration.
10. **Why can a healthy worker hide failure?** One task can be failed or stale while the worker process runs. Monitor useful data movement and task-level errors.



---

# Chapter 15 — Kafka operations, security, capacity, and disaster recovery

1. **Does a running broker mean a healthy service?** No. Critical partitions or consumer dependencies may be unavailable. Start with useful business progress.
2. **Under-replicated versus unavailable?** Under-replication means fewer healthy in-sync copies than desired; unavailability means required operations cannot currently proceed for the affected partition. Severity depends on the remaining protection and workload.
3. **Why doesn't adding a broker immediately fix skew?** Existing replicas and leaders need placement changes, and a hot key remains one ordering domain. Capacity and distribution are different problems.
4. **Why throttle reassignment?** Moving replicas competes with client traffic and replication. Controlled movement preserves headroom while completing the operation.
5. **TLS versus SASL versus ACL?** Encryption, identity, and permission are separate layers. A securely encrypted connection can still be overprivileged.
6. **Why inspect advertised listeners?** Clients discover broker addresses after bootstrap. An address reachable only inside a container network breaks external clients even if bootstrap succeeds.
7. **Is mirroring zero-loss DR?** Not by default. Asynchronous replication can lag, and recovery needs coordinated authorities and checkpoints. State measured RPO rather than assuming zero.
8. **Why is a restore drill necessary?** Backups and replicas are only potential recovery inputs. A drill tests credentials, tools, ordering, data integrity, and elapsed recovery time.
9. **Can lowering durability settings solve an incident?** It may increase acceptance while changing loss guarantees. That is a business trade-off requiring an explicit decision, not an invisible tuning fix.
10. **What does a senior incident update contain?** User impact, known evidence, current hypothesis, mitigation, risk, next checkpoint, and owner. Avoid claiming a root cause before the evidence supports it.



---

# Chapter 16 — MongoDB Read Models, Queries, and Recovery


1. **When embed?** Embed bounded values read and changed with the parent when ownership and lifecycle align. Reference independently growing or reused data. Explain the worst-case document shape, not only one sample order.

2. **Does flexible schema eliminate migrations?** No. Readers still depend on field meaning and shape. Version the projection, support transition, and rebuild or backfill deliberately. Missing and null values need explicit interpretation.

3. **What is atomic in MongoDB?** A single-document write has its documented atomicity; multi-document work requires a supported transaction or another correctness protocol. Do not extend that guarantee to PostgreSQL or external HTTP effects.

4. **Why use owner versions?** They let a full-snapshot projection reject duplicates and older state from the same ordered source. They do not compare unrelated services. Delta events need gap handling instead of unconditional skipping.

5. **Read concern versus read preference?** Concern specifies the read guarantee, while preference guides eligible node selection. A secondary choice and a consistency level solve different questions. Evaluate both against the user's freshness requirement.

6. **Does write-concern timeout mean no write?** No. Acknowledgment conditions were not satisfied in time, but the effect may exist. Inspect by stable identity and desired state; blind creation of a new identity can duplicate work.

7. **Why can sharding make queries worse?** A query without a useful routing key may involve multiple shards and merge work. Distribution also creates skew and operational complexity. Choose a key from real access patterns and growth.

8. **How diagnose a slow aggregation?** Examine stage semantics, selectivity, index support, data volume, and execution statistics. Move filters only when equivalence is preserved. Do not assume the shortest pipeline is the cheapest.

9. **Can change streams replace business events?** They report database changes, which may not align with public domain meaning or expose the desired contract. A transformation can be appropriate, but ownership, schema, replay, and retention still need design.

10. **How rebuild a projection safely?** Create a replacement view, backfill from a consistent source protocol, consume subsequent changes, validate, and switch readers. Include deletes, schema version, and rollback. Merely replaying “whatever events remain” is insufficient.



---

# Chapter 17 — Redis: Caches, Expiration, Leases, and Recovery


1. **TTL versus eviction?** TTL is an expiration policy attached to a key; eviction responds to memory policy. A key can disappear for either reason. Code must handle misses, and business deadlines must not depend solely on exact expiry notifications.

2. **Why can cache-aside return stale data after invalidation?** An earlier reader can fill an old value after the writer deletes the cache. Define acceptable staleness or use version-aware repair. Deleting after commit reduces some races but does not create atomicity across stores.

3. **What causes a stampede?** Many callers miss together and duplicate expensive loads. Coalesce by key, add jitter where useful, bound refresh concurrency, and protect the owner. A distributed lease is an optimization whose failure policy must be explicit.

4. **Why compare a lease token before deletion?** A previous lease can expire and a new owner can acquire the key. Plain deletion by the old owner would remove the new owner's lease. Atomic compare/delete prevents that specific error.

5. **Does token-safe release provide fencing?** No. It protects lease deletion, not stale writes to another resource. Fencing requires an ordered token and enforcement by the protected resource. Random tokens identify owners but do not order them.

6. **Can Redis guarantee no overselling here?** Redis is deliberately advisory. PostgreSQL's transaction and constraints decide reservation. This avoids making inventory correctness depend on cache expiry, eviction, replication lag, or lease timing.

7. **What happens during Redis outage?** A naive miss fallback can overload the database. Bound fallback, preserve useful stale data only where allowed, and shed excess demand. Recovery includes gradual refill, not merely reconnecting clients.

8. **RDB or AOF?** They offer different persistence/restart trade-offs and can be configured according to data importance. Choose from recovery objectives and verify actual loss/restore behavior. A cache may prefer rebuild, while business state cannot assume loss is harmless.

9. **Sentinel versus Cluster?** Sentinel supports monitoring/failover for non-clustered Redis; Cluster distributes key ownership and introduces slot/routing constraints. Neither replaces a business consistency design. Choose topology from scale and availability needs.

10. **Is Pub/Sub enough for invalidation?** Disconnected subscribers can miss publications. Use TTL/version repair or a durable mechanism for required eventual invalidation. Best-effort notification alone cannot promise every cache sees every change.



---

# Chapter 18 — DDD, Service Ownership, and Read/Write Models


1. **What is a bounded context?** It is a boundary within which a model and language have consistent meaning. It may be a module before becoming a service. Explain a word such as Product that legitimately differs across contexts.

2. **What makes an aggregate useful?** It identifies state whose rules are enforced together through a root. It is not all related data. A smaller coherent invariant boundary reduces unnecessary loading and contention while preserving correctness.

3. **Domain service versus application service?** A domain service expresses business behavior that does not fit one object; an application service coordinates a use case and resources. Keeping the distinction visible makes domain rules easier to test without infrastructure.

4. **Why choose a modular monolith?** It can enforce boundaries while retaining simpler deployment and local calls. Choose it when independent scaling/releases do not justify distributed costs. Modularity requires interface discipline even in one process.

5. **Does database-per-service mean a different server?** The essential property is ownership and controlled access, though physical isolation can serve operational needs. Sharing infrastructure does not authorize cross-service table writes. Independent schema and credentials can help enforce ownership.

6. **Does CQRS require event sourcing?** No. Separate command/query models can use ordinary current-state persistence. Event sourcing chooses authoritative event history. Combining them can be useful but adds separate complexity from each choice.

7. **Why is an outbox not automatically an event store?** It records publication obligations alongside current-state changes. Its retention and schema may not support full aggregate reconstruction. An event-sourced system requires explicit authoritative history semantics.

8. **Orchestration or choreography?** Compare visibility, coupling, workflow complexity, and ownership. This course chooses Order orchestration so pending steps and compensation are inspectable. Participants still own their rules and databases.

9. **Can compensation restore the exact past?** Often no: a refund differs from never charging, and released stock may be acquired by another buyer. Define business-valid recovery and track its failures. A saga is not a time machine.

10. **Why use an anti-corruption layer?** External meanings may differ from the local model. Translation prevents provider states, units, and failure codes from leaking everywhere. It must preserve uncertainty rather than forcing unknown states into a convenient enum.



---

# Chapter 19 — Networking, Gateways, Discovery, and Time Budgets


1. **What does DNS success prove?** It provides a name-resolution result under the resolver's rules. It does not prove target health or current membership. Inspect caches and TTL behavior when failover appears inconsistent across clients.

2. **Does TCP acknowledgment prove payment success?** No. TCP concerns bytes and transport state. The application may not have committed, or may have committed before a response was lost. Business identity and reconciliation resolve that uncertainty.

3. **Does TLS replace authorization?** No. It secures/authenticates the channel under a trust configuration. The application still decides whether a principal may access a particular order. Gateway authentication does not excuse missing service-level ownership checks.

4. **Why reuse connections?** Reuse avoids repeated setup and can reduce latency/resource cost. Pools require bounds, idle/lifetime policy, and correct response cleanup. Reusing a stale connection can still fail and must fit the retry contract.

5. **HTTP/2 versus HTTP/3?** HTTP/2 multiplexes streams over its connection transport; HTTP/3 maps HTTP over QUIC. Their transport differences do not remove backend queues or business ambiguity. Check actual negotiation at each hop.

6. **Client-side versus server-side balancing?** Client-side code chooses an instance from discovery information; server-side infrastructure chooses behind a stable endpoint. Both can use stale health knowledge. Explain selection, connection reuse, and failure handling together.

7. **Timeout versus deadline?** A timeout bounds one wait, while a deadline bounds useful completion for the operation. Independent full timeouts at every hop can exceed the end-to-end budget. Propagate remaining time and preserve ambiguous outcomes.

8. **Rate limit versus concurrency limit?** Rate limits control arrivals over time; concurrency limits control simultaneous work. When dependency latency rises, the same rate can create much more in-flight work. Use the limit matching the resource risk.

9. **Can forwarded headers be trusted?** Only through a defined trusted proxy boundary that sanitizes client-supplied values. Otherwise a client may spoof its apparent origin. Identity-sensitive policy should use authenticated context.

10. **Why can gateway retry be dangerous?** The first upstream attempt may commit before its response disappears. Repeating a mutation without stable identity can duplicate effects. Retry only when the operation contract and remaining budget justify it.



---

# Chapter 20 — Docker: Packaging, Isolation, and Running Java Reliably

1. **How does an image differ from a container?** An image is packaged content and metadata; a container adds execution and mutable state. Replacing the runtime should preserve required state through an external durable store.
2. **Why can localhost fail between services?** Each container normally has its own network namespace. Use the peer's service name and port; verify host and container routes separately.
3. **Does EXPOSE publish a port?** No. Publication is runtime networking configuration. The distinction matters when reviewing accidental exposure of internal APIs.
4. **Why order Dockerfile instructions carefully?** Stable dependency work can remain cached when application source changes. Cache correctness still depends on declared inputs and pinned external dependencies.
5. **Why use multi-stage builds?** Build tools can remain in a builder stage while the runtime receives only the executable and required runtime. This reduces unnecessary content without proving application security.
6. **Why can Java die below its heap limit?** Native allocations and other processes contribute to container memory. The kernel can terminate the container even while heap use appears acceptable.
7. **Is a named volume a backup?** No. It survives container replacement but shares storage failure and operator-deletion risks. A backup needs independent recovery evidence.
8. **Why did Kafka bootstrap succeed but publishing fail?** Metadata may advertise unreachable broker addresses. Examine every returned address from the client's network, rather than repeatedly changing bootstrap ports.
9. **Does depends_on guarantee availability?** It can establish startup ordering and configured health dependencies, but runtime outages still occur. Applications need bounded recovery and durable progress.
10. **What makes an image release reproducible?** Record source, dependencies, base and application digests, build tooling, and configuration differences. Reusing a tag alone cannot prove identical content.



---

# Chapter 21 — Kubernetes: Reconciliation, Workloads, and Application Recovery


1. **What does reconciliation mean?** Controllers repeatedly compare desired and observed state and act to reduce differences. It is not a guarantee that all dependencies are healthy. Explain which controller owns the object and which application obligation remains outside it.

2. **Pod versus Deployment?** A Pod is an execution/scheduling unit; a Deployment manages replaceable replicas and rollout behavior through controllers. Deleting a managed Pod can trigger replacement. Editing one transient Pod is not a durable deployment strategy.

3. **Why is a StatefulSet insufficient for PostgreSQL HA?** Stable identity and volume association do not implement replication, safe promotion, or backup. Database consistency and failover require a supported operating design. Kubernetes supplies primitives, not the database protocol.

4. **Readiness versus liveness?** Readiness controls ordinary traffic eligibility; liveness asks whether restart can help. A remote dependency outage often should not restart every process. Define the decision from the service's acceptance contract.

5. **Why can Java be OOM-killed with heap remaining?** Container memory includes more than heap. Native buffers, metadata, threads, and agents contribute. Compare process/container evidence and effective JVM settings before increasing Xmx.

6. **Why does a Service have no endpoints?** Selectors may not match labels, Pods may be unready, or endpoint state may not yet reflect changes. Inspect each boundary. A declared port alone does not prove a listening healthy process.

7. **Does HPA guarantee overload recovery?** Scaling requires metrics, capacity, startup time, and available downstream resources. More Pods can intensify a database bottleneck. Use bounded admission and a scale signal matched to the limiting resource.

8. **What does a PVC guarantee?** It requests storage under the cluster's binding/provisioning model. Durability, attachment, zone behavior, and backup depend on the actual storage system and policy. A bound claim is not a tested restore.

9. **Why review Helm output?** Templates and values can render unexpected selectors, privileges, or resources. Rendering is the concrete deployment proposal. A successful chart installation does not prove business correctness or safe data migration.

10. **Does a Job run a business operation exactly once?** No. Failure and retry can repeat execution. Use durable identity, checkpoints, and constraints appropriate to the operation. Scheduling machinery cannot infer whether a remote effect committed.



---

# Chapter 22 — AWS, Azure, Terraform, Cost, and Disaster Recovery


1. **What remains our responsibility with managed services?** Application correctness, identity policy, relevant configuration, data access, and recovery validation remain. The provider's scope differs by service. A managed database does not prevent a duplicate authorization caused by application retries.

2. **Zones versus regions?** They represent different geographic and failure scopes in each provider's model. Multi-zone deployment addresses some local failures; regional recovery needs a broader plan. Verify service support rather than assuming every region offers every redundancy mode.

3. **Is a private subnet sufficient security?** No. Routes, addresses, firewall/security rules, DNS, endpoints, and application identity all matter. Explain a permitted packet path and an authorized resource operation separately.

4. **Why use workload identity?** It avoids distributing long-lived static credentials and supports scoped temporary access. Trust configuration and least privilege still matter. A short-lived administrator token remains overprivileged.

5. **Is every RDS Multi-AZ deployment a read replica?** No; deployment forms have different capabilities. Inspect the exact product mode and engine documentation. Availability and read scaling are separate requirements.

6. **What is Terraform state for?** It binds configuration addresses to real resource identities and records management metadata. Resources can continue existing if state is lost, but safe management becomes harder. Protect and recover state deliberately.

7. **Does state locking prevent all drift?** It protects supported concurrent state operations, not every manual cloud change. Reconcile drift with context, particularly after emergency repairs. Do not treat the lock as a universal cloud mutex.

8. **Does sensitive=true keep secrets out of state?** Not generally. It can redact presentation while the value remains in artifacts. Use supported secret-handling features and protect state/plan access rather than relying on display suppression.

9. **RPO versus RTO?** RPO bounds acceptable lost progress; RTO bounds acceptable recovery delay. Both are objectives that drills must test. A backup schedule and an infrastructure diagram do not constitute measured recovery evidence.

10. **Why is multi-region active/active difficult for orders?** Concurrent authorities can disagree about reservations, payment operations, and workflow state. Define ownership, conflict resolution, and fencing. Traffic distribution alone does not solve data consistency.



---

# Chapter 23 — Security: Identity, Authorization, Tokens, and Trust Boundaries


1. **Authentication versus authorization?** Authentication establishes identity evidence; authorization evaluates an action on a resource. A valid token and broad scope do not establish order ownership. Show the tenant/object check, not only the login screen.

2. **OAuth versus OIDC?** OAuth defines delegated access roles and tokens; OIDC adds an authentication layer and ID token for the client. An ID token is not interchangeable with an API access token. Validate purpose and audience.

3. **Why use PKCE?** It binds the authorization request to code redemption using challenge/verifier material, reducing intercepted-code misuse. It does not make a browser-held client secret confidential or replace redirect/issuer validation and other flow protections.

4. **What must a JWT validator check?** Trusted signature/key and allowed algorithm, issuer, intended audience, time validity, token purpose, and application claim semantics. Decoding is not validation. A valid signature from an unrelated issuer or for another API is insufficient.

5. **Can offline JWT validation instantly revoke tokens?** Not by signature checking alone. Revocation/freshness needs a policy such as short lifetime, introspection, or another stateful mechanism. Explain the availability and latency trade-offs honestly.

6. **Why is CORS not access control?** It governs browser cross-origin behavior, not every HTTP client. Attackers can send requests outside the browser model. Enforce authentication and resource authorization regardless of CORS settings.

7. **Can stateless APIs have CSRF risk?** Yes, if the browser automatically supplies their credentials, such as cookies. Assess the credential mechanism and request flow. Returning JSON or avoiding server sessions does not alone answer the CSRF question.

8. **What does mTLS prove?** It authenticates channel peers according to certificate trust and protects transport. Permission still requires policy, and token binding requires the relevant protocol. A valid service certificate does not authorize arbitrary inventory changes.

9. **How rotate secrets without downtime?** Design supported overlap or refresh, deploy consumers that can obtain new credentials, verify use, then retire old material. The exact sequence depends on the system. Never claim rotation is complete solely because a new value was stored.

10. **How turn a threat model into evidence?** For each threat, name a control and a negative test or observable property. For cross-tenant access, test altered IDs, exports, and subscriptions with distinct tenants. A diagram without verification is only a proposal.



---

# Chapter 24 — Observability, SLOs, and evidence-led diagnosis

1. **Which signal should I use?** Metrics reveal aggregate changes; logs explain discrete outcomes; traces connect causality and timing. Link them without forcing every identifier into every metric.
2. **Why not label by orderId?** Each distinct ID expands the time-series set. Search logs/traces by ID and keep metric labels bounded.
3. **Why is202 latency insufficient?** It measures acceptance. A durable asynchronous workflow also needs completion/freshness SLIs.
4. **Can we average pod p99 values?** Generally no. Percentiles do not combine that way; aggregate compatible histograms first.
5. **Does a successful span prove commit?** Only if the instrumented boundary includes that commit. A method that returns before asynchronous completion can be misleading.
6. **What is an error budget?** The allowed bad service under the SLO over its window. It guides reliability priorities, not permission to ignore failures.
7. **Why preserve context through Kafka?** It connects asynchronous causation. Keep business correlation separately because traces have different sampling and retention.
8. **What if export fails?** Bound buffering and monitor drops/backpressure. Diagnostic export must not create an uncontrolled application outage.
9. **How do you investigate latency?** Establish scope/time, correlate rates/errors/traces/saturation, form a hypothesis, and test a targeted change. Avoid random tuning.
10. **What makes an alert useful?** Timely action can reduce user impact, and the alert identifies an owner and evidence. Non-actionable pages create fatigue.



---

# Chapter 25 — Testing, Maven, CI/CD, and safe releases

1. **Why aren't mocks enough?** They implement assumptions you programmed. They cannot reveal real SQL, locking, serialization, or broker coordination behavior.
2. **Why not make every test end-to-end?** Such tests are slower, harder to diagnose, and vulnerable to unrelated failures. Use them for critical journeys while focused tests cover mechanisms.
3. **Does JUnit6 require new package names?** Jupiter APIs remain under org.junit.jupiter. Use the approved managed versions and a compatible runtime/build plugin rather than mixing artifacts manually.
4. **What does Testcontainers prove?** Real service integration under the container's configuration. It does not automatically prove multi-zone resilience or production storage durability.
5. **Why use bounded polling?** Asynchronous completion time varies. Wait for the required condition within a deadline and report current evidence when it fails.
6. **What is a contract test?** A check of assumptions exchanged between components, including schema and semantics. It helps prevent independent deployments from breaking each other.
7. **Why can a green build be misleading?** Tests may not be discovered, integration phases may not run, or assertions may only check mocks. Inspect report counts and evidence.
8. **Why promote one artifact?** Rebuilding changes inputs and can produce a different binary. A digest ties validation to the deployed object.
9. **Why isn't rollback always easy?** Data/schema/events may have changed irreversibly. Design compatibility windows and forward recovery before release.
10. **How do you handle flaky tests?** Classify the nondeterminism, preserve diagnostics, fix timing/isolation assumptions, and track quarantine ownership if necessary. Repeated retries alone hide lost confidence.



---

# Chapter 26 — Distributed-systems theory applied to real decisions

1. **What does CAP actually constrain?** Simultaneously guaranteeing the formal availability and linearizability properties during partitions. It is not a general three-checkbox technology ranking.
2. **Why add PACELC?** Even healthy distributed communication has latency costs. Failure-time trade-offs do not describe every normal-operation decision.
3. **Does R+W>N prove linearizability?** It proves an intersection under specific membership assumptions. Correct ordering/version/recovery behavior still needs an appropriate protocol.
4. **Consensus versus two-phase commit?** Consensus agrees on values/log history despite specified failures; atomic commit coordinates a transaction's decision across participants. They solve related but different problems.
5. **Why can a lease holder become stale?** It may pause beyond expiry and resume after replacement. The protected resource must reject obsolete ownership where required.
6. **Do timestamps establish causality?** Wall clocks may disagree; logical clocks provide particular ordering properties. A numerical comparison alone does not identify every causal relationship.
7. **Why is timeout not failure proof?** The operation may have committed and lost its response. Preserve operation identity and reconcile uncertain outcomes.
8. **Why can retries worsen outages?** They add work to an already constrained dependency, especially when multiplied across layers. Bound attempts and elapsed time.
9. **What does a bulkhead protect?** A defined pool/concurrency budget, limiting how one failing dependency consumes shared resources. It needs bounded queues and useful rejection behavior.
10. **How do you choose consistency?** Start from the business invariant and failure consequence, then choose authority and protocol. Explain what users see during isolation and recovery.



---

# Chapter 27 — Performance engineering, queueing, and capacity

1. **Why isn't high throughput sufficient?** Work may be queued, dropped, or completed with wrong outcomes. Measure useful completion, latency, errors, and invariants together.
2. **What does Little's Law tell us?** Average work in a defined stable system relates to effective arrival rate and average residence time. It is a consistency check and sizing aid, not a tail-latency prediction.
3. **Can low CPU coexist with overload?** Yes. Threads may wait for locks, connections, network, or quotas. Inspect queues and dependency saturation.
4. **Why not increase every pool?** More concurrency can amplify contention and resource exhaustion. Pools should protect actual downstream capacity.
5. **Why use JMH?** JVM optimization, warmup, and measurement overhead can invalidate casual timing loops. The harness reduces pitfalls but does not replace representative workload design.
6. **Allocation versus leak?** Allocation creates objects; a leak retains unnecessary reachable objects. Their diagnostic evidence and remedies differ.
7. **Why inspect EXPLAIN ANALYZE carefully?** It executes the statement. A useful diagnostic on a safe read can be harmful when applied casually to a mutating query.
8. **Open versus closed load?** Independent arrivals can maintain pressure when the system slows; waiting users reduce subsequent arrivals. Choose the model matching real demand.
9. **Why can a cache worsen recovery?** Cold misses or synchronized expiration can overload the database. Include warmup, stampede control, and degraded behavior in tests.
10. **What belongs in a performance claim?** Workload, environment, versions, duration, metrics, correctness evidence, comparison, and limitations. A single best run is weak evidence.



---

# Chapter 28 — High-Level Design: turning requirements into a defensible system

1. **Where do you start?** With users, operations, invariants, and workload assumptions. Naming databases first can lock the conversation into unnecessary machinery. For checkout, distinguish resource acceptance from fulfillment completion and identify the atomic stock rule before drawing services.
2. **How accurate must estimates be?** Accurate enough to reveal dominant constraints and guide measurements. Keep units visible and perform sensitivity analysis. A factor-of-ten uncertainty in event size may matter more than rounding average RPS to a decimal.
3. **Why PostgreSQL for orders?** Our order write model needs transactional updates, constraints, and indexed lookup. This is a workload-specific recommendation, not a claim that document databases cannot transact. Explain failure recovery and operational skill alongside features.
4. **Why Kafka here?** Independent durable consumption and replay are useful for orchestration results and projections. The cost is schema, lag, retention, and operations. A simpler queue remains a valid alternative if replay and multiple independent subscribers are unnecessary.
5. **How do you prevent overselling?** Enforce availability and reservation creation in the Inventory owner with appropriate local concurrency control and retry policy. A preliminary GET stock is advisory; correctness must hold at the write.
6. **Can Gateway enforce all security?** No. Services must validate trusted identity and resource-specific permissions. Internal reachability is not sufficient authorization, and direct service access or compromised peers must not bypass ownership checks.
7. **What does cancellation mean?** Define legal states and compensation semantics. Before dispatch it might release inventory and void authorization; after dispatch it may become a return process. A single CANCELLED flag cannot erase external effects.
8. **How do you scale a hot product?** First measure lock/queue behavior and bound work. Partitioning by an unrelated key cannot remove the shared invariant. Options include serialized reservation ownership or carefully allocated stock pools, each with explicit trade-offs.
9. **What if a region fails?** Start with RPO/RTO and authority. A failover that permits two writers can violate invariants. Explain replication lag, fencing, routing, credentials, and reconciliation instead of only adding another region to the drawing.
10. **When would you change the design?** Record thresholds in ADRs: workload measurements, team ownership, unmet SLOs, or operational cost. Choosing a simpler first design with an observable migration trigger is stronger than premature complexity.



---

# Chapter 29 — Low-Level Design: responsibilities, invariants, and change

1. **What is good encapsulation?** Expose operations that preserve meaning, not arbitrary setters. An Order should transition through business operations. Private fields with setters can still permit impossible combinations and do not prove a sound model.
2. **When use an interface?** When it captures a stable capability, meaningful variation, or external boundary. A PaymentGateway is useful for provider semantics and tests; an interface mirroring every trivial class creates navigation cost without clear value.
3. **State versus Strategy?** State changes behavior according to lifecycle; Strategy chooses a policy. Payment retry eligibility might depend on workflow state, while a provider-selection strategy is an independent choice. Combining them carelessly can hide illegal transitions.
4. **Why composition?** It permits focused collaborators and explicit substitution without inheriting unrelated behavior. Inheritance remains valid when substitutability is real. Explain the actual variation rather than claiming one mechanism is always superior.
5. **How ensure thread safety?** Identify mutable state and ownership first. Immutability, confinement, locks, atomics, or database concurrency may fit different boundaries. Synchronizing a bean does not coordinate another application instance.
6. **What is a rich domain model?** It holds behavior that protects domain rules. It is not a model that knows HTTP, SQL, Kafka, and UI details. Application coordination and adapters should remain distinct from business state.
7. **How model failures?** Preserve distinctions that affect action: invalid request, legal-state conflict, unavailable dependency, and unknown outcome. A generic RuntimeException or boolean can erase information needed for retry and reconciliation.
8. **How test extensibility?** Add a concrete variation, such as a pending payment provider, and observe which files change. If orchestration branches on provider names everywhere, the boundary is weak. Test behavior and contracts rather than class-count targets.
9. **When is a pattern harmful?** When its indirection exceeds the variability or risk it addresses. A full State hierarchy for three simple transitions may be less clear than an enum. Patterns are vocabulary for decisions, not mandatory decorations.
10. **How handle late payment success?** Reload authoritative workflow state and correlate the operation. If cancellation has begun, schedule the permitted compensation instead of moving to READY. Record the action durably and tolerate duplicate results.



---

# Chapter 30 — Failure engineering: prove recovery, not just restart

1. **What is your first action in an incident?** Establish customer impact, current scope, and safe stabilization while preserving evidence. Do not immediately restart every dependency. A bounded mitigation buys time to diagnose without multiplying changes.
2. **Which logs matter?** Correlated operation/event IDs, state transitions, errors with causes, and timing around boundaries. Logs without stable identity cannot distinguish a retry from a new operation. Avoid sensitive payment/customer data.
3. **How prove no duplicate payment?** Query the authoritative business operation ledger/provider result, not only consumer logs. Two distinct messages may request the same payment. Define the invariant and inspect durable effects after recovery.
4. **Why not retry everything?** Validation failures do not become valid with delay, and unknown outcomes can already have effects. Classify errors, preserve identity, bound attempts, and expose exhausted or unresolved work.
5. **Can you reset offsets to fix lag?** Resetting changes what will be read, not whether business work is correct. It can skip required effects or repeat dangerous ones. First determine whether the backlog is valid and design replay/idempotency.
6. **What if the DLQ grows?** Treat it as owned unresolved work. Classify causes, alert on age and volume, repair contracts/data safely, and replay with preserved identity. A DLQ is not a business completion state.
7. **How distinguish deadlock from slow SQL?** Inspect waits, competing transactions, plans, and timing. A deadlock is a cycle of dependencies; slow execution may consume resources without a cycle. Use evidence rather than the same timeout fix for both.
8. **Why can recovery overload the system?** Replayed work adds to ongoing arrivals, often with cache misses and downstream writes. Limit drain concurrency and protect live traffic. Calculate net recovery capacity rather than assuming restart clears backlog instantly.
9. **What belongs in a postmortem?** Impact, evidence-backed timeline, contributing conditions, mitigation, recovery verification, and owned corrective actions. Avoid speculative blame. Distinguish confirmed causes from remaining hypotheses.
10. **What if compensation fails?** Persist the failed compensation and retry/escalate under policy. Do not mark cancellation complete while inventory or authorization remains unresolved. Recovery needs a visible owner and auditable state.



---

# Chapter 31 — Technical interviews, engineering communication, and project evidence


1. **Tell me about your Kafka experience.** A truthful answer separates professional usage from project practice and conceptual learning. Describe the exact lab, failure, and test you performed; then explain a lesson such as publication uncertainty. Do not claim broker operations at production scale without having done them.

2. **Why did you choose an outbox?** Our local database transaction must persist both the order and the intent to publish. A relay can retry after failure, so duplicate publication remains possible and consumers need protection. Explain the simpler direct-publish failure window and the storage/relay cost.

3. **What would you change at ten times load?** First identify the bottleneck through telemetry and load tests. Hot keys, database locks, pool limits, and partition assignment may dominate different operations. Scaling every service uniformly wastes resources and may worsen a constrained database.

4. **Describe a production incident.** Use a real incident and make your contribution explicit. Explain customer impact, evidence, mitigation, root cause, and prevention. If you worked as part of a team, say which actions were yours and which were shared. Avoid invented outcome metrics.

5. **How do you handle a question whose default you cannot recall?** State the mechanism you understand, identify the version sensitivity, and explain how you would verify the default and effective setting. Guessing a number confidently can invalidate otherwise sound reasoning.

6. **How would you prevent duplicate payments?** Define the business operation and stable identity; enforce durable uniqueness; detect conflicting request reuse; coordinate with provider idempotency/status lookup; and reconcile unknown outcomes. A consumer offset or event ID alone does not solve every payment duplicate.

7. **How do you know a system is recovered?** Dependencies are available, backlog and unresolved workflows are within objectives, and authoritative business invariants pass checks. A green dashboard with stale business state is insufficient. Name the queries and operation IDs you would inspect.

8. **Why not use microservices everywhere?** They offer ownership and deployment independence at the cost of distributed coordination and operations. Compare with a modular monolith for the actual team and workload. Choosing fewer services can be a deliberate senior decision.

9. **How do you mentor an engineer?** Use a real example: establish the concept gap, demonstrate a mechanism, let them attempt a task, review evidence, and gradually increase ownership. Do not measure mentoring only by how much explanation you delivered; describe the learner's observable capability.

10. **What are this project's limitations?** State implementation and verification status precisely. Local labs do not prove multi-zone operations, real financial compliance, provider guarantees, or production throughput. Explain what additional evidence would be needed and why it matters.

Each answer is a study model. Interactive mocks use fresh questions and withhold the answer until the learner responds.

