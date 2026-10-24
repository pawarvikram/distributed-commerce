# Source ledger

Sources opened 2026-10-24. RFCs are explicit protocol references, with later updates possible; only the named foundational mechanisms are used. Live Spring Cloud Gateway pages show 5.0.4, with discovery page 5.0.3; Commons shows 5.0.4. These do not establish compatibility with proposed Boot 4.1.1. No Cloud release train is selected. Java remains 21 and Maven the project build tool.

| ID | Source | Sections |
|---|---|---|
| N1 | [DNS concepts, RFC 1034](https://www.rfc-editor.org/rfc/rfc1034.html) | Names, resolvers, caching |
| N2 | [TCP, RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html) | Connections, reliability, flow control |
| N3 | [TLS 1.3, RFC 8446](https://www.rfc-editor.org/rfc/rfc8446.html) | Handshake, authentication, early data |
| N4 | [HTTP/1.1, RFC 9112](https://www.rfc-editor.org/rfc/rfc9112.html) | Framing, connection management |
| N5 | [HTTP/2, RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html) | Streams and multiplexing |
| N6 | [HTTP/3, RFC 9114](https://www.rfc-editor.org/rfc/rfc9114.html) | HTTP over QUIC |
| N7 | [Forwarded header, RFC 7239](https://www.rfc-editor.org/rfc/rfc7239.html) | Proxy information and trust |
| N8 | [Additional status codes, RFC 6585](https://www.rfc-editor.org/rfc/rfc6585.html) | 429 |
| N9 | [Gateway request flow](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway-server-webflux/how-it-works.html) | Matching and filters |
| N10 | [Gateway timeouts](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway-server-webflux/http-timeouts-configuration.html) | Global and per-route settings |
| N11 | [Discovery route locator](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway-server-webflux/the-discoveryclient-route-definition-locator.html) | Registry-derived routes |
| N12 | [RequestRateLimiter](https://docs.spring.io/spring-cloud-gateway/reference/spring-cloud-gateway-server-webflux/gatewayfilter-factories/requestratelimiter-factory.html) | Key resolution and limiting |
| N13 | [Spring LoadBalancer](https://docs.spring.io/spring-cloud-commons/reference/spring-cloud-commons/loadbalancer.html) | Client-side instance selection |
| N14 | [Java HttpRequest builder](https://docs.oracle.com/en/java/javase/21/docs/api/java.net.http/java/net/http/HttpRequest.Builder.html) | Request timeout |
| N15 | [Java HttpClient builder](https://docs.oracle.com/en/java/javase/21/docs/api/java.net.http/java/net/http/HttpClient.Builder.html) | Connect timeout and proxy selection |
| N16 | [JDK HttpServer](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.httpserver/com/sun/net/httpserver/HttpServer.html) | Binding, handlers, executor |

Protocol mechanisms are documented facts; deadline allocations, deployment choices, failure labs, and commerce policies are original recommendations. No book claims or measured latency results.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed versions, executions, or measured results are claimed. Verified date: 2026-10-24.

