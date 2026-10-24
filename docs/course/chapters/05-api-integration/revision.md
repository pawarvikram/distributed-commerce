# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Specify semantics before transport. Separate resource creation, durable acceptance, and completed business work. Preserve identity across retries, define compatibility and error behavior, budget deadlines, and make stream/callback recovery explicit.


## Correct these misconceptions
REST is not simply a naming convention for controllers. HTTP success is not workflow completion. Idempotency is not deduplication in a process-local map. gRPC does not remove partial failure. GraphQL does not guarantee cheap queries. A live socket is not durable delivery. Broker acknowledgment does not imply a downstream business transaction finished.


## Retrieval assessment
Unanswered: a payment callback arrives twice, an order-status stream disconnects, and the original order POST response was lost. Explain the independent recovery mechanism at each boundary, including durable identity, state transitions, and the customer's eventual view.


## Evidence and practice
The OpenAPI example was not parsed, generated, or served. No HTTP/gRPC/GraphQL server or broker was started. Sources were checked on 2026-10-24. Expected observations are design predictions. A future implementation must test duplicate concurrent POSTs, lost responses, payload mismatch, authorization, malformed input, deadline expiry, and schema compatibility.


Consult the [source ledger](source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.

