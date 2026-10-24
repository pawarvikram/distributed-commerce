# Source ledger

Opened 2026-10-24. These are selected official specifications and project documentation, not a claim to have read complete standards. OpenAPI 3.1.1 and Jakarta Messaging 3.1 are explicit reference versions; no corresponding dependency installation is implied. Java remains 21, Maven the build tool, and Boot 4.1.1 the proposed application baseline.

| ID | Reference | Sections used |
|---|---|---|
| A1 | [HTTP semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) | Methods, conditional requests, 201/202 |
| A2 | [Problem Details, RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html) | Problem members, security considerations |
| A3 | [OpenAPI 3.1.1](https://spec.openapis.org/oas/v3.1.1.html) | Paths, operations, schemas, responses |
| A4 | [gRPC concepts](https://grpc.io/docs/what-is-grpc/core-concepts/) | Unary and streaming RPC lifecycle |
| A5 | [gRPC deadlines](https://grpc.io/docs/guides/deadlines/) | Deadline propagation and cancellation |
| A6 | [Protobuf proto3](https://protobuf.dev/programming-guides/proto3/) | Field numbers, reserved fields, updating types |
| A7 | [GraphQL queries](https://graphql.org/learn/queries/) | Selection sets, arguments, variables |
| A8 | [WHATWG server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html) | EventSource, event ID, reconnection |
| A9 | [WebSocket RFC 6455](https://www.rfc-editor.org/rfc/rfc6455.html) | Handshake, frames, close and control frames |
| A10 | [Jakarta Messaging 3.1](https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1.html) | Queue/topic models, acknowledgments |
| A11 | [RabbitMQ AMQP model](https://www.rabbitmq.com/tutorials/amqp-concepts) | AMQP 0-9-1 exchanges, bindings, queues |
| A12 | [GitHub webhook practices](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks) | Secure, prompt, deduplicated handling |
| A13 | [Java 21 HttpClient](https://docs.oracle.com/en/java/javase/21/docs/api/java.net.http/java/net/http/HttpClient.html) | Client reuse, synchronous/asynchronous calls |
| A14 | [Spring response bodies](https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-controller/ann-methods/responsebody.html) | Message conversion |

GitHub-specific webhook documentation is an example provider contract, not a universal retry guarantee. Live Spring docs are Framework 7.0.10; exact proposed-Boot compatibility remains unverified. Protocol statements below are documented facts; commerce policies and payloads are recommendations.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed versions, executions, or measured results are claimed. Verified date: 2026-10-24.

