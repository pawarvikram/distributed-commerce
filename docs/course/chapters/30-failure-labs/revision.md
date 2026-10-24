# Chapter 30 revision

Sequence: baseline -> hypothesis -> precise fault -> evidence -> restore -> reconcile -> invariant checks -> report.

Know the difference: fault/symptom/cause; mitigation/fix; restart/recovery; message duplicate/business duplicate; prediction/observation.

Five critical boundaries: before DB commit, after commit before publish, after publish before relay marker, after consumer DB commit before offset, after external effect before response.

Practice the 25 labs only in their required isolated environments. Single-broker downtime is not replicated leader failover. Process stop is not every network partition. Never reset offsets or delete history merely to clear a dashboard.

Evidence: operation IDs, state queries, telemetry, exact fault, restored invariants. All labs are procedures, not executed results.

