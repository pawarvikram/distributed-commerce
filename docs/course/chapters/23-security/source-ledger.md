# Source ledger

Opened 2026-10-24. Spring Security live pages report 7.1.2; proposed Boot remains 4.1.1 and its managed dependencies are unvalidated here. Java 21/Maven remain agreed. OAuth security guidance includes RFC 9700 (2025), updating earlier OAuth guidance. OIDC Core is 1.0 with errata set 2. OWASP Top 10:2025 was verified; ASVS and cheat sheets are rolling references, not a claim of full compliance.

| ID | Official source | Sections |
|---|---|---|
| S1 | [Spring Security architecture](https://docs.spring.io/spring-security/reference/servlet/architecture.html) | FilterChainProxy, SecurityFilterChain |
| S2 | [JWT resource server](https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html) | Decoding, validation, authorities |
| S3 | [Method security](https://docs.spring.io/spring-security/reference/servlet/authorization/method-security.html) | Interception and authorization |
| S4 | [CSRF](https://docs.spring.io/spring-security/reference/servlet/exploits/csrf.html) | Browser credentials and protections |
| S5 | [CORS](https://docs.spring.io/spring-security/reference/servlet/integrations/cors.html) | Browser preflight integration |
| O1 | [OAuth 2.0, RFC 6749](https://www.rfc-editor.org/rfc/rfc6749.html) | Roles and grant model |
| O2 | [OAuth security BCP, RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html) | Current flow/security recommendations |
| O3 | [PKCE, RFC 7636](https://www.rfc-editor.org/rfc/rfc7636.html) | Challenge/verifier binding |
| O4 | [OIDC Core](https://openid.net/specs/openid-connect-core-1_0.html) | ID token, authentication, validation |
| O5 | [JWT BCP, RFC 8725](https://www.rfc-editor.org/rfc/rfc8725.html) | Algorithm, issuer/audience, token confusion |
| O6 | [Introspection, RFC 7662](https://www.rfc-editor.org/rfc/rfc7662.html) | Token active-state query |
| O7 | [OAuth mTLS, RFC 8705](https://www.rfc-editor.org/rfc/rfc8705.html) | Client authentication and bound tokens |
| T1 | [TLS 1.3, RFC 8446](https://www.rfc-editor.org/rfc/rfc8446.html) | Channel protection and authentication |
| W1 | [OWASP authorization](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html) | Deny by default, per-request permission |
| W2 | [Secrets management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) | Lifecycle, rotation, access |
| W3 | [Threat modeling](https://cheatsheetseries.owasp.org/cheatsheets/Threat_Modeling_Cheat_Sheet.html) | Assets, boundaries, threats, mitigation |
| W4 | [ASVS](https://asvs.owasp.org/), [Top 10:2025](https://top10.owasp.org/2025/) | Verification framework and risk awareness |
| W5 | [SSRF prevention](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html) | Destination controls |
| W6 | [SQL injection prevention](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html) | Parameterization |
| W7 | [Password storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html) | Adaptive password hashing |
| W8 | [XSS prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross_Site_Scripting_Prevention_Cheat_Sheet.html) | Contextual encoding and sanitization |

Documented protocol/framework facts are cited. Commerce policies, example identities, threat scenarios, and tests are course recommendations. No book reading, audited compliance, or successful penetration test is claimed.


## Evidence boundaries
Only named sections and retrieved content support documented facts. Designs, examples, diagrams, exercises, and arithmetic are original educational synthesis. No books, installed versions, infrastructure actions, executions, compliance audits, or measured results are claimed. Verified date: 2026-10-24.

