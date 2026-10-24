# Source ledger

Verified 2026-10-24. Java 21 is the baseline; structured concurrency and scoped values are outside the stable-code examples. Spring Boot remains proposed 4.1.1; live Spring scheduling documentation is Framework 7.0.10 and is conceptual evidence only.

| ID | Reference | Sections |
|---|---|---|
| C1 | [JLS 17](https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html) | 17.4.5 happens-before, 17.5 final fields |
| C2 | [Concurrent package](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/package-summary.html) | Executors, queues, synchronizers, memory consistency |
| C3 | [ThreadPoolExecutor](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html) | Queueing and rejection |
| C4 | [CompletableFuture](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html) | Execution policy, cancellation, timeouts |
| C5 | [Virtual threads](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html) | Scheduling, pinning, adoption |
| C6 | [AtomicInteger](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/AtomicInteger.html) | Atomic updates and compare-and-set |
| C7 | [ReentrantLock](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/locks/ReentrantLock.html) | Interruptible/timed locking, fairness |
| C8 | [Spring scheduling](https://docs.spring.io/spring-framework/reference/integration/scheduling.html) | Executors and async interception |

Documented API facts are cited. All stock rules, queue capacities, failure sequences, and experiments are original recommendations with illustrative numbers. No throughput claim has been measured.


## Evidence boundaries
Only the named sections and retrieved content support documented facts. Commerce designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed dependency versions, executions, or benchmark results are claimed. Examples require separate verification. Verified date: 2026-10-24.

