# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](notes.md); read its mechanisms, examples, ten explained interview answers, and solved exercises.

## Core model
Validate identity evidence in its issuer, audience, purpose, and time context. Enforce action/resource/tenant policy at the owning service. Protect browser and input boundaries, manage secrets throughout their lifecycle, and verify threat mitigations with explicit negative cases and safe failure behavior.


## Correct these misconceptions
A valid JWT is not universal authorization. Signed does not mean encrypted. An ID token is not an API access token. CORS is not a firewall. Stateless does not automatically mean CSRF-safe. mTLS does not replace business policy. A secret manager does not justify broad access. A successful happy-path login is not a security test suite.


## Retrieval assessment
Unanswered: an authenticated support agent requests another tenant's export, a service presents a valid certificate but the wrong token audience, and key discovery is temporarily unavailable. Explain the independent decisions, safe responses, audit evidence, and recovery policy for each case.


## Evidence and practice
Official references were opened; no Java execution, identity-provider flow, token validation, certificate rotation, Spring context, scanner, or penetration test was run. Expected output is predicted. OWASP Top 10 provides risk awareness and ASVS supports verification requirements; mentioning them does not establish compliance. A later implementation must test authentication negatives, object/tenant authorization, browser flows, key rotation, secrets, and safe failure behavior.


Consult the [source ledger](source-ledger.md) before version-specific claims. Draw trust and failure boundaries, then identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.

