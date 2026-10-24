# Revision sheet

Prepared 2026-10-24. This is a recall aid; use the full [chapter](notes.md) for mechanisms, code, ten explained interview answers, and solved exercises.

## Core model
Trace object ownership, proxy crossings, execution threads, and physical resource transactions. Use auto-configuration as explainable conditional assembly. Keep business state and outgoing obligations durable, and make startup and shutdown part of the architecture.


## Correct these misconceptions
An annotation does not execute itself. A bean is not automatically thread-safe. An async task is not a durable event. A transaction does not include arbitrary remote resources. Reactive return types do not transform blocking calls. Graceful shutdown is not a replacement for crash recovery. Boot conventions still need inspection when dependencies change.


## Retrieval assessment
Unanswered: a singleton OrderService stores the current request in a field, invokes a private transactional helper, and calls an async payment method on itself. Identify three distinct mechanisms that fail to match the developer's expectation and propose a corrected boundary design.


## Evidence and practice
References were opened and version banners recorded. The single-file Java example, Maven build, Spring contexts, transaction integration, and shutdown behavior were not executed. Treat the code as a mechanism demonstration. A future implementation needs integration tests proving rollback, proxy traversal, outbox atomicity, and restart behavior on the selected database and reviewed dependency set.


Read the [source ledger](source-ledger.md) before making version-specific claims. Explain the mechanism aloud, draw the failure boundary, and state what evidence would falsify your diagnosis. Reading alone does not establish mastery.

