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

