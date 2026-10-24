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

