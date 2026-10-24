# Start here — a route from beginner understanding to engineering evidence

This course contains31 substantive chapters for one Enterprise Distributed Commerce, Inventory, Payment and Fulfillment Platform. There is no dedicated DSA track. Java21, Maven, Spring Boot, Kafka/KRaft, PostgreSQL, MongoDB, Redis, Docker, Kubernetes, cloud, security, observability, testing, performance, HLD, and LLD remain in scope. JUnit6 managed by Boot is the agreed testing direction.

The local project is C:\Projects\distributed-commerce. The chapter library lives in docs/course/chapters. Begin at [Chapter1](chapters/01-foundations/notes.md), use the [glossary](glossary.md) when a term is unfamiliar, and return to the [index](README.md) to navigate.

## What these notes provide
Each chapter starts with prerequisites and objectives, then explains the problem, mechanism, implementation, failure behavior, trade-offs, and verification. It includes diagrams, ten answered study questions, a worked exercise, and an unanswered assessment. A separate revision sheet supports recall.

Examples have explicit scope. A complete standalone Java class is not necessarily a complete service. A Kubernetes manifest requiring a built image is not a deployed application. An expected observation is a prediction to test, not a recorded result.

Reading can build a strong mental model. Proficiency requires explaining that model without notes, implementing it, observing failures, and correcting your own mistakes. The course tracks that evidence separately from the availability of written material.

## If you are new to backend engineering
Start with these concrete ideas:

- A program is instructions and data. A process is one running instance with its own memory and resources. A thread is an execution path inside that process.
- A Java class defines a type. An object is an instance. A method performs an operation. An exception signals an abnormal outcome in code; it does not automatically undo external work.
- An API is a contract for interaction. An HTTP API sends requests and responses across a network. A successful network response and a correct business outcome are separate claims.
- A database stores durable structured state. A table holds rows; a key identifies them; a constraint prevents invalid states. A transaction groups changes under specific atomicity/isolation rules.
- A service is a software capability with an interface and ownership. A microservice deployment introduces process/network boundaries, even when the business rules look like ordinary method calls.
- A broker stores/routes messages. Kafka retains partitioned logs so independent readers can track their own progress.
- A build converts source and dependencies into an artifact. Deployment runs a chosen artifact with configuration and resources. Neither proves the business workflow is correct.

If these terms are unfamiliar, read the first-principles sections twice and use the paper exercises before installing infrastructure. You do not need a running Kubernetes cluster to understand why a lost payment response is ambiguous.

## Follow one order through the course
Customer C100 submits an order with request key R100. Order validates the request, stores it, and creates an outbox event in one local PostgreSQL transaction. The relay publishes that durable intent to Kafka. Inventory processes the event, applies a unique reservation operation, and emits its result. Order advances the Saga and requests a simulated payment under a stable operation ID.

When payment is known to be authorized and stock is reserved, fulfillment can proceed. Rejection triggers a defined compensation. A timeout creates an uncertain state requiring reconciliation. Shipping, Notification, and Reporting observe the relevant durable outcomes.

This same story explains why we need transaction boundaries, event schemas, idempotency, retries, state machines, security, metrics, and recovery. Keep returning to “what is durably known now?” and “who is authorized to change this state?”

## Four passes through a chapter
1. **Understand:** read sections1–8, define unfamiliar words, and explain the diagram in your own language.
2. **Apply:** study sections9–15, trace the code/configuration, and identify its assumptions and failure windows.
3. **Retrieve:** close the notes, answer the ten study questions, then compare with the model answers.
4. **Prove:** solve the exercise, perform the specified lab when prerequisites exist, record actual output, and answer the fresh assessment.

Do not copy a model answer and mark yourself ready. A useful response explains the mechanism, gives a concrete example, identifies a failure, and compares an alternative.

## Suggested16-week pacing
The original15–20hours/week baseline remains adjustable. More time is appropriate for a beginner; chapter count is not a deadline.

| Week | Main chapters | Evidence target |
|---|---|---|
|1|1–3|Trace failure boundaries and Java concurrency|
|2|4–5|Explain Spring request/transaction paths and API choices|
|3|6|Prove SQL constraints, locking, and query behavior|
|4|7–8|Trace Kafka storage and producer acknowledgment|
|5|9–11|Prove consumer recovery and listener error handling|
|6|12|Implement durable outbox/inbox/Saga boundaries|
|7|13–15|Test schema evolution and explain Streams/operations|
|8|16–17|Model documents and cache consistency|
|9|18–19|Defend service ownership and network paths|
|10|20–22|Package, deploy in a lab, and explain cloud recovery|
|11|23–24|Prove authorization and observe useful outcomes|
|12|25 and27|Build reliable tests and measured performance evidence|
|13|26 and28|Defend consistency and complete HLD scenarios|
|14|29|Implement stateful, concurrency-aware LLD exercises|
|15|30|Run selected failure labs and reconcile outcomes|
|16|31|Review gaps, practice interviews, assemble honest evidence|

All chapters are available ahead of teaching because you requested a complete notes library. Teaching and mastery still progress according to your answers and lab evidence.

## Tools without confusion
PowerShell commands execute in a working directory. A relative path is resolved from that directory; an absolute path names a location directly. Inspect before changing. On Linux, path spelling and case may differ from Windows.

Git records source history, not every external state change. A commit does not back up your database or cloud account. Maven resolves dependencies and runs configured build phases. A successful package phase does not prove integration tests ran; inspect test reports.

Use these read-only orientation commands in the actual project when available:

```powershell
Get-Location
Get-ChildItem
git status --short
java --version
javac --version
mvn --version
docker version
```

These commands are learning instructions, not a claim they all succeeded. The previous environment check verified Java21; Maven/Docker/application runtime remain separate verification tasks. See the existing [Windows environment notes](../windows-environment.md).

Do not run a cloud apply, destructive volume removal, production offset reset, or failure injection simply because it appears in a lab. Read that lab's prerequisites, use its isolated environment, and verify its target. The course does not require paid cloud deployment.

## Reading code critically
For each example, identify imports/dependencies, input, output, state, error paths, and lifecycle. Mark which changes are local and which cross a network. Ask whether a crash after each line could leave a durable effect.

A database example needs a real database to prove SQL behavior. A local Java simulation can demonstrate a state rule but cannot establish distributed consensus. A mocked provider can prove retry identity handling but not a real provider's settlement guarantees.

The code is embedded teaching material. Most examples have not been compiled or executed. [Verification status](verification-report.md) records document checks; it does not claim application tests passed.

## Keep useful personal notes
For every chapter, record:
- My explanation in five sentences.
- The diagram I can draw without looking.
- The mistake I initially made and the correction.
- The lab command/configuration, actual observation, and interpretation.
- The business invariant checked.
- The unanswered question and next experiment.

Use the [lab report template](lab-report-template.md). Review difficult topics after one day, three days, one week, and two weeks from the actual study date. Do not mark a review as completed merely because its date arrived.

## Mastery levels
Not started means no study evidence. Learning means you can explain some ideas with help. Practiced means you have implemented or experimentally verified important behavior. Interview ready means you can explain, draw, implement, troubleshoot, compare alternatives, and defend trade-offs in a fresh scenario. Needs revision records a demonstrated gap.

No chapter is marked interview ready just because this book exists. [Progress](progress.md) keeps that distinction explicit.

## Optional scope
Angular/TypeScript frontend work remains optional and secondary to backend competence; no React track is added. Search infrastructure is optional where the data chapter discusses it. Paid cloud deployment and production operations are not required to read or complete local conceptual exercises.

The approved technical books remain supplementary recommendations. The source ledger does not claim unavailable book chapters were read. Use the linked official references to deepen a mechanism and reverify changing API/configuration behavior before implementation.

