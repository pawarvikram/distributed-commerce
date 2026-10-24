# Source ledger

All links opened 2026-10-24. Live pages report Framework 7.0.10 and Boot 4.1.2. The agreed proposed Boot baseline remains 4.1.1; no silent upgrade or compatibility claim is made. Example concepts are stable, but exact dependencies and configuration must be validated when implementation is authorized.

| ID | Primary source | Relevant section |
|---|---|---|
| S1 | [Container overview](https://docs.spring.io/spring-framework/reference/core/beans/basics.html) | ApplicationContext and metadata |
| S2 | [Bean lifecycle](https://docs.spring.io/spring-framework/reference/core/beans/factory-nature.html) | Initialization, destruction, lifecycle |
| S3 | [Proxying mechanisms](https://docs.spring.io/spring-framework/reference/core/aop/proxying.html) | JDK/CGLIB, self-invocation |
| S4 | [Boot auto-configuration](https://docs.spring.io/spring-boot/reference/using/auto-configuration.html) | Conditions, backoff, debug report |
| S5 | [DispatcherServlet](https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-servlet.html) | Front controller and delegates |
| S6 | [WebFlux overview](https://docs.spring.io/spring-framework/reference/web/webflux/new-framework.html) | Reactive concurrency model |
| S7 | [Transactional annotations](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html) | Proxy mode, rollback, settings |
| S8 | [Propagation](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/tx-propagation.html) | REQUIRED and REQUIRES_NEW |
| S9 | [Scheduling and async](https://docs.spring.io/spring-framework/reference/integration/scheduling.html) | TaskExecutor and async advice |
| S10 | [Graceful shutdown](https://docs.spring.io/spring-boot/reference/web/graceful-shutdown.html) | Grace periods and server behavior |

Documented mechanisms are attributed to these pages. Transaction placement, package boundaries, and commerce workflows are course design recommendations. No book chapter is claimed as consulted evidence.


## Evidence boundaries
Only the named sections and retrieved content support documented facts. Commerce designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No book chapters, installed dependency versions, executions, or benchmark results are claimed. Examples require separate verification. Verified date: 2026-10-24.

