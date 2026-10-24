# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](notes.md); read its mechanisms, examples, ten explained interview answers, and solved exercises.

## Core model
Kubernetes reconciles infrastructure objects; applications preserve business obligations. Choose controllers by lifecycle, probes by decision, resources by evidence, and storage by recovery requirements. Review rendered deployment changes and test failure behavior across both platform and business boundaries.


## Correct these misconceptions
Running is not the same as ready. Readiness is not completed business work. A StatefulSet is not a database backup. A namespace alone does not deny network traffic. A memory limit is not an Xmx setting. HPA does not create unlimited database capacity. Helm does not reverse irreversible data changes. Restarting a Pod does not deduplicate a payment.


## Retrieval assessment
Unanswered: a rollout creates three new Pods that pass liveness but fail readiness, old Pods approach memory limits, and the database is at its connection ceiling. Explain the control-plane state, immediate recovery choices, and the capacity/schema evidence needed before proceeding.


## Evidence and practice
Official sources and version banners were checked. The YAML was not parsed by kubectl, applied, rendered through Helm, or exercised against an image. No cluster exists by implication. Future verification must include schema validation on the selected version, readiness, selector routing, rollout capacity, shutdown timing, OOM behavior, and outbox/inbox recovery under forced termination.


Consult the [source ledger](source-ledger.md) before version-specific claims. Draw failure boundaries and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.

