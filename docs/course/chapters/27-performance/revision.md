# Chapter27 revision

Throughput=useful completions/time; latency=service+waiting. Little's Law: L=lambda×W with matching stable-system boundaries. Backlog drain=backlog/(capacity-arrivals) when the difference is positive. Pools bound concurrency; queues defer work. Open/closed load models expose different behavior. JMH addresses JVM measurement pitfalls; JFR helps find runtime cost.

Practice: calculate queue fill/drain; diagnose low CPU with high latency; compare allocation with retention; write a complete benchmark report without invented results.

Checklist: [ ] estimates [ ] profiling [ ] real load model [ ] correctness checks [ ] before/after evidence [ ] assessment. Examples unexecuted.

