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
