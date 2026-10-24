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

