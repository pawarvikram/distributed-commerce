# Revision sheet

Prepared 2026-10-24. This is a recall aid; use the full [chapter](notes.md) for mechanisms, code, ten explained interview answers, and solved exercises.

## Core model
Model values deliberately; use Java 21's stable type features to make invalid states harder to express. Separate language guarantees, JVM specification, and HotSpot implementation choices. Investigate whole-process memory and correlate runtime evidence with business latency. Repair ownership and lifecycle mistakes before treating tuning flags as a solution.


## Correct these misconceptions
GC does not remove every object that is no longer useful to the business. A record is not recursively immutable. More heap is not always lower latency. A Java source feature available in a later tutorial may be preview or absent in 21. A successful local demonstration does not prove a service's transaction safety. A dump is evidence requiring interpretation, not a diagnosis by itself.


## Retrieval assessment
Unanswered: Order retains completed futures in a map, request latency rises, and its container restarts while heap occasionally drops. Describe how you would distinguish reachable retention, native-memory pressure, and a slow dependency. State which observation would falsify each hypothesis. No score is recorded until an answer and supporting evidence exist.


## Evidence and practice
No Java code or diagnostic command was executed during authoring. Expected program output is `O-1001 lines=1`, followed by `Reserved: R-1001`. Run `java OrderValueDemo.java` with a confirmed Java 21 installation. This single-file exercise does not require or replace the Maven project.

Illustrative diagnostic sequence, unexecuted: `jcmd <pid> help`, then `jcmd <pid> help JFR.start` and the relevant command help before capture. For example, a bounded recording can use `jcmd <pid> JFR.start name=checkout settings=profile duration=60s filename=checkout.jfr`. Inspect command impact and permissions on the actual target. The jcmd manual documents target access and available-command discovery. [J12] There are no measured performance results or claims of application readiness.


Read the [source ledger](source-ledger.md) before making version-specific claims. Explain the mechanism aloud, draw the failure boundary, and state what evidence would falsify your diagnosis. Reading alone does not establish mastery.

