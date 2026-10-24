# Chapter7 revision — Kafka architecture

| Term | Precise meaning | Frequent mistake |
|---|---|---|
| Topic | Named stream divided into partitions | Assuming one global order |
| Partition | Ordered log with its own offsets | Treating a key as a uniqueness constraint |
| Offset | Logical position within a partition | Treating it as a byte address or order ID |
| Replica | Copy of partition data | Confusing it with a consumer group |
| ISR | Current in-sync membership | Assuming it always equals replication factor |
| High watermark | Replication commitment boundary | Confusing it with consumer progress |
| KRaft quorum | Metadata agreement | Assuming controllers store all user payloads |
| Compaction | Eventual removal of superseded keyed values | Expecting immediate uniqueness |
| Tombstone | Null-valued deletion marker under compaction semantics | Dropping it before a projection sees deletion |

Trace: metadata discovery → partition leader → batch append → follower replication → visibility under acknowledgment/isolation rules → consumer processing → independent checkpoint.

Storage estimate: ingress bytes/sec × retention seconds × replication, adjusted for measured compression, indexes, internal data, growth, and headroom. Recovery needs spare disk/network too.

Failure traps: unreachable advertised addresses; full disk; retention shorter than recovery; replicas sharing one failure domain; controller quorum healthy while a data partition is unavailable. Eligible leader replicas require feature-aware reasoning in Kafka4.2; do not use an outdated ISR-only election slogan.

Retrieval questions:
1. Draw three partitions with independent leaders and three replicas each.
2. Explain why a consumer can fall behind retention despite successful producer acknowledgments.
3. Contrast an append-only order history with a compacted current-order view.
4. Explain logical offsets, sparse indexes, segments, and page cache.
5. Predict one-zone loss with healthy controllers but insufficient data replicas.

Mastery evidence: [ ] explain without notes [ ] draw [ ] inspect metadata [ ] run a multi-broker failure lab [ ] verify business event identities [ ] answer assessment. No runtime evidence or mastery is recorded.

