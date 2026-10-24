# Beginner's glossary

Read definitions in context; a short definition is a starting point, not a substitute for the mechanism and failure examples in its chapter.

| Term | Plain explanation | Chapter |
|---|---|---|
| Process | A running program with memory and operating-system resources |2,20|
| Thread | An execution path inside a process |3|
| Heap | JVM memory region holding most objects |2|
| Stack | Per-thread execution frames and local call state |2|
| Garbage collection | Reclaiming memory for objects no longer needed/reachable under the runtime model |2|
| JIT | Runtime compilation of frequently executed code |2|
| JFR | JVM event recording for diagnosis |2,27|
| Race condition | Outcome depends on uncontrolled timing of concurrent actions |3|
| Atomic operation | An indivisible operation within a specified boundary |3,6|
| Visibility | Whether one execution observes another's writes |3|
| Happens-before | A defined ordering/visibility relationship, not merely wall-clock order |3,26|
| Backpressure | Limiting upstream work when downstream cannot keep up |3,9|
| Virtual thread | Lightweight Java thread suited to many waiting tasks |3|
| Dependency injection | Supplying collaborators instead of constructing them inside business code |4|
| Bean | Object managed by the Spring container |4|
| Proxy | Wrapper that intercepts calls to add behavior |4|
| Transaction manager | Component coordinating a transaction for particular resources |4,6|
| HTTP | Request/response protocol used for web communication |5,19|
| REST | Architectural constraints for resource-oriented interaction |5|
| Idempotency | Repetition preserves the defined effect rather than creating another unintended one |5,12|
| Serialization | Encoding values as bytes |8,13|
| Contract | Agreed structure, meaning, and behavior at a boundary |5,13|
| gRPC | RPC framework commonly using Protobuf contracts |5|
| GraphQL | Query language/runtime contract for selecting API data |5|
| SSE | Server-to-client event stream over HTTP |5|
| WebSocket | Persistent bidirectional communication channel |5|
| Webhook | Server callback to another system over HTTP |5|
| JMS | Java messaging API specification with queue/topic models |1,5|
| ACID | Atomicity, consistency, isolation, durability of transactions under defined guarantees |6|
| MVCC | Maintaining versions to support concurrent database access |6|
| Index | Auxiliary structure helping locate data under supported access patterns |6,16|
| Query plan | Database's chosen execution strategy |6,27|
| Lock | Coordination restricting conflicting access |6|
| Deadlock | Cyclic waiting where participants cannot progress without intervention |6,30|
| Optimistic locking | Detecting conflicting change, commonly using an expected version |6|
| JPA | Java persistence specification |6|
| Hibernate | ORM implementation that can implement JPA |6|
| Persistence context | Managed entity identity/state within a persistence scope |6|
| Flush | Synchronizing pending ORM changes with database operations |6|
| Topic | Named Kafka stream divided into partitions |7|
| Partition | Independently ordered Kafka log |7|
| Offset | Logical position in one partition |7,9|
| Replica | Another copy of partition data |7|
| ISR | Current set of replicas considered in sync |7|
| KRaft | Kafka's metadata quorum architecture |7|
| Retention | Policy controlling how long/how much data is retained |7|
| Compaction | Eventual removal of superseded keyed records |7|
| Tombstone | Deletion marker, commonly a null value in a compacted topic |7,14|
| Producer | Client publishing Kafka records |8|
| Acknowledgment | Evidence of a defined processing/storage stage, not necessarily business completion |8|
| Batch | Multiple records processed/transmitted together |8|
| Consumer group | Independent logical subscriber whose members divide work |9|
| Commit offset | Persist a consumer's recovery checkpoint |9|
| Rebalance | Change partition ownership among group members |9|
| Lag | Difference between relevant production/log and consumer progress; define exactly which |9,24|
| DLT | Dead-letter topic holding work that needs a different recovery decision |11|
| Outbox | Durable event intent committed with local business state |12|
| Inbox | Durable record of processed event identities |12|
| Saga | Multi-step workflow of local transactions with compensation/recovery |12|
| Compensation | New action counteracting a previous business effect |12|
| Reconciliation | Comparing durable evidence to resolve incomplete or uncertain outcomes |12,30|
| Schema | Data shape and interpretation contract |13|
| Backward compatibility | New reader can interpret older data in the stated model |13|
| Forward compatibility | Older reader can interpret newer data in the stated model |13|
| CDC | Capturing committed data changes from a source system |12,14|
| Connector | Configurable integration moving data into/out of Kafka |14|
| KStream | Stream of event occurrences |14|
| KTable | Evolving keyed current-state view |14|
| Changelog | Retained updates used to rebuild state |14|
| Window | Time-based grouping rule for stream processing |14|
| RPO | Acceptable data-loss horizon in a defined disaster scenario |15,22|
| RTO | Target time to restore the defined useful service |15,22|
| Document | Structured nested record used by document databases |16|
| Shard | Partition of a dataset assigned to part of a system |16|
| TTL | Time-to-live/expiry rule; precise deletion timing depends on system |17|
| Eviction | Removing cached data under a resource policy |17|
| Cache-aside | Application reads cache and loads misses from the authority |17|
| Stampede | Many cache misses simultaneously overload the backing service |17|
| Lease | Time-bounded ownership claim under stated assumptions |17,26|
| Bounded context | Domain boundary within which terms/models have a consistent meaning |18|
| Aggregate | Domain consistency boundary for related state and behavior |18,29|
| CQRS | Separating command/write and query/read models |18|
| Event sourcing | Reconstructing authoritative state from an event history |18|
| DNS | Mapping names to network information |19|
| TCP | Reliable ordered byte-stream transport with its own connection semantics |19|
| TLS | Cryptographic channel protection and peer-authentication mechanisms |19,23|
| Gateway | Edge routing/policy component for API traffic |19|
| Load balancer | Distributes traffic among eligible destinations |19|
| Container | Isolated process environment built from packaged content |20|
| Image | Packaged filesystem/runtime metadata |20|
| Volume | Storage with a lifecycle distinct from one container |20|
| Pod | Kubernetes scheduling/execution unit containing cooperating containers |21|
| Deployment | Controller managing desired application replicas and rollouts |21|
| Service | Kubernetes stable discovery/routing abstraction over selected endpoints |21|
| Readiness | Whether an instance should receive relevant traffic |21|
| Liveness | Whether restarting an instance is appropriate under the configured probe |21|
| RBAC | Permissions assigned through roles and bindings |21,23|
| Terraform state | Mapping/metadata connecting configuration to managed resources |22|
| IAM | Identity and access management |22,23|
| Authentication | Establishing who an actor is |23|
| Authorization | Deciding what that actor may do to a resource |23|
| OAuth | Framework for delegated authorization |23|
| OIDC | Identity layer for authentication built on OAuth2 |23|
| JWT | Signed/encrypted-token format family; validation and purpose still matter |23|
| mTLS | TLS with mutual certificate-based peer authentication |23|
| Metric | Numeric observation aggregated over time/dimensions |24|
| Trace/span | Causal operation record and one timed component within it |24|
| Cardinality | Number of distinct metric-label combinations |24|
| SLI/SLO | Measurement and its target over a window |24|
| Error budget | Allowed bad service under an SLO |24|
| Mock | Programmed substitute for a collaborator in a test |25|
| Integration test | Test crossing a real selected component boundary |25|
| Contract test | Test of assumptions exchanged between components |25|
| CI/CD | Continuous integration and controlled delivery/deployment practices |25|
| Linearizability | Operation behavior respecting a single real-time-consistent order |26|
| Serializability | Transaction behavior equivalent to a serial execution |26|
| Quorum | Required participant set for an operation |26|
| Consensus | Agreement on values/history under a specified failure model |26|
| Fencing token | Monotonic ownership evidence checked by the protected resource |26|
| Circuit breaker | Temporarily limits dependency calls after failures |26|
| Bulkhead | Resource isolation preventing one workload from consuming everything |26|
| Throughput | Useful completed work per unit time |27|
| Latency | Elapsed time for one operation |27|
| Percentile | Threshold locating observations within a distribution |24,27|
| Saturation | Demand reaches/exceeds a resource's useful capacity |27|
| HLD | Architecture-level components, contracts, ownership, scale, and trade-offs |28|
| LLD | Concrete types, interactions, invariants, and implementation behavior |29|
| Runbook | Repeatable diagnosis/recovery procedure with evidence and limits |30|
| ADR | Record of an architectural decision, context, alternatives, and consequences |28,31|

