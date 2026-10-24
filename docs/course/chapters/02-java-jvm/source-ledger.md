# Source ledger

Opened 2026-10-24; section-level evidence, not claims of reading entire manuals. Java 21 remains the language/runtime baseline. Maven remains the project build tool; Spring Boot 4.1.1 remains proposed. Nothing here validates its dependency graph.

| ID | Official reference | Sections used |
|---|---|---|
| J1 | [Java 21 language changes](https://docs.oracle.com/en/java/javase/21/language/java-language-changes-release.html) | Final and preview features |
| J2 | [Record classes](https://docs.oracle.com/en/java/javase/21/language/records.html) | Components, constructors |
| J3 | [Sealed classes](https://docs.oracle.com/en/java/javase/21/language/sealed-classes-and-interfaces.html) | Permitted implementations |
| J4 | [Pattern matching switch](https://docs.oracle.com/en/java/javase/21/language/pattern-matching-switch.html) | Type coverage, null |
| J5 | [SequencedCollection API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/SequencedCollection.html) | Encounter order, reversed view |
| J6 | [JVMS chapter 2](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-2.html) | 2.5 runtime areas, 2.6 frames |
| J7 | [JVMS chapter 5](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-5.html) | Loading, linking, initialization |
| J8 | [HotSpot enhancements](https://docs.oracle.com/en/java/javase/21/vm/java-hotspot-virtual-machine-performance-enhancements.html) | Tiered compilation, escape analysis |
| J9 | [GC introduction](https://docs.oracle.com/en/java/javase/21/gctuning/introduction-garbage-collection-tuning.html) | Reclamation and trade-offs |
| J10 | [G1 collector](https://docs.oracle.com/en/java/javase/21/gctuning/garbage-first-g1-garbage-collector1.html) | Regions, evacuation, pause goals |
| J11 | [Diagnostic tools](https://docs.oracle.com/en/java/javase/21/troubleshoot/diagnostic-tools.html) | JFR, native memory tracking, dumps |
| J12 | [jcmd](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html) | Help, recording and dump commands |

Language/JVM statements below are documented facts tied to these sources. Commerce designs, incident sequences, and suggested experiments are course recommendations and original examples. Some OpenJDK JEP links returned 403; the Oracle Java 21 language guides provide the usable evidence. No unavailable ZGC page is treated as verified.


## Evidence boundaries
Only the named sections and retrieved content support documented facts. Commerce designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed dependency versions, executions, or benchmark results are claimed. Examples require separate verification. Verified date: 2026-10-24.

