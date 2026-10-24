# Source ledger

Checked: 2026-10-19. Live reference pages can change; resolved POMs and image digests must be recorded when the first application is built.

| Topic | Official source | Evidence / limitation |
|---|---|---|
| Boot / Java / Maven | https://docs.spring.io/spring-boot/system-requirements.html | Boot 4.1.1 supports Java 21; Maven minimum 3.6.3 |
| Managed dependencies | https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html | Core framework/client/testing versions in checkpoint |
| Plugin ownership | https://docs.spring.io/spring-boot/appendix/dependency-versions/properties.html | Compiler/Surefire/Failsafe managed properties; exact resolved values pending |
| Cloud alignment | https://spring.io/projects/spring-cloud/ | 2025.1.x supports Boot 4.1.x starting 2025.1.2; page identifies 2025.1.3 |
| Kafka 4.2 container | https://kafka.apache.org/42/getting-started/quickstart/ | Official apache/kafka:4.2.2 image; local pull and integration unexecuted |
| Testcontainers Kafka module | https://java.testcontainers.org/modules/kafka/ | Verify 2.x artifacts/API during implementation |
| Maven | https://maven.apache.org/download.cgi | Current Maven 3.10.0 listed |
| PostgreSQL | https://www.postgresql.org/docs/release/ | Server patch selection deferred |
| MongoDB | https://www.mongodb.com/docs/manual/release-notes/8.0/ | Proposed 8.0 family; exact image pending |
| Redis | https://redis.io/docs/latest/operate/oss_and_stack/install/install-stack/docker/ | Docker installation source; exact image pending |
| Java distribution | https://adoptium.net/temurin/releases/?version=21 | Candidate distribution; exact patch not verified |
| Kubernetes | https://kubernetes.io/releases/ | Tooling selection deferred until deployment milestone |
| kind | https://kind.sigs.k8s.io/docs/user/quick-start/ | Local-cluster option; not installed |
| Helm | https://helm.sh/docs/intro/install/ | Installation reference; not installed |

Books remain recommended reading, not consulted chapter evidence.
