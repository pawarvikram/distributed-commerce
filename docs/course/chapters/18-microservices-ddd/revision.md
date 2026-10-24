# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Use business meaning and invariants to choose boundaries. Keep modules coherent before distributing them. Separate command models, read models, and authoritative histories. Make workflow ownership, late-event handling, and compensating obligations durable and explicit.


## Correct these misconceptions
DDD is not a requirement to create many services. A bounded context is not always one container. An aggregate is not an entire object graph. Sharing a database server does not imply shared write ownership. CQRS is not event sourcing. Publishing events is not proof of reconstructible history. Compensation is not global rollback.


## Retrieval assessment
Unanswered: Reporting shows READY while Order is CANCELLING, Inventory has reserved stock, and Payment's authorization result is unknown. Identify authoritative owners, valid customer communication, required reconciliation, and why a dashboard join cannot resolve the workflow by itself.


## Evidence and practice
Sources were checked; the Java program, module checks, state persistence, migration, and saga were not executed. Expected output is a prediction. The example covers only readiness evidence. Production cancellation, concurrent updates, event authenticity, durable deduplication, and reconciliation remain separate implementation verification tasks.


Consult the [source ledger](source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.

