# Revision sheet

Prepared 2026-10-24. Recall aid for the full [chapter](notes.md); read its mechanisms, examples, ten explained interview answers, and exercise solutions.

## Core model
Use Redis where copied or temporary state has explicit semantics. Plan misses, invalidation races, expiry, eviction, and cold recovery. Separate owner tokens from fencing and cache availability from business authority. Measure the load that escapes the cache.


## Correct these misconceptions
Cache memory is not free or infinite. TTL is not a precise durable timer. Lua atomicity does not span PostgreSQL. A lock key does not stop a paused old worker from writing elsewhere. Replication is not a historical backup. A high hit ratio does not prove safe outage capacity. Redis availability does not authorize stock changes.


## Retrieval assessment
Unanswered: a cache refresh lease expires during a JVM pause, a second worker refreshes newer data, and Redis then fails over. Describe the safe responsibilities of each worker and the owner database. Explain which errors are acceptable duplicate work and which would violate a business invariant.


## Evidence and practice
Official pages were opened; Redis commands, Lua, failover, persistence restore, and Spring adapters were not executed. No server/client release was installed or selected. Expected return values depend on completing the lease steps before expiry. The example verifies only a local key protocol if run, not distributed lease safety or authoritative commerce correctness.


Consult the [source ledger](source-ledger.md) before version-specific claims. Draw the failure boundary, explain the mechanism aloud, and identify evidence that could falsify your diagnosis. Reading alone does not establish mastery.

