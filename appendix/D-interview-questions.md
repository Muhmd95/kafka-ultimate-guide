# Appendix D — Interview Questions & Answers

> The most commonly asked Kafka questions, grouped beginner → advanced. Answers are deliberately one-to-five lines: what an interviewer actually wants to hear.

---

## Beginner

**1. What is Kafka?**
A distributed event streaming platform: an append-only, replicated log that stores events and lets producers write and consumers read them — usable as a message queue, pub/sub system, or stream-processing backbone.

**2. What is a topic vs a partition?**
Topic = the logical stream name. Partition = the physical append-only log on a broker that stores the data; one topic has many partitions spread across brokers. Topics organize data; partitions scale it.

**3. How does Kafka preserve ordering?**
Only within a partition. All messages with the same key hash to the same partition (`murmur2(key) % N`), so per-key order is preserved; there is no cross-partition ordering.

**4. What is an offset?**
The sequential position of a record in a partition. Consumers track their position via offsets committed to Kafka, enabling restart/resume.

**5. What is a consumer group?**
A set of consumers sharing a group ID; each partition of the topic is assigned to exactly one member — the group behaves like a work queue. Different groups each consume the full stream (pub/sub).

**6. Producer vs consumer — who drives?**
Consumers pull at their own pace; Kafka doesn't push. Pull gives natural backpressure and lets each consumer control its position.

**7. Why is Kafka fast?**
Sequential disk I/O on append-only logs, batching everywhere, zero-copy sends, reliance on OS page cache, and partition-level parallelism.

## Intermediate

**8. How does replication work?**
Each partition has a leader (serves all reads/writes) and followers (passively replicate). The controller assigns leaders and balances them; on leader failure an in-sync follower is promoted.

**9. What is the ISR?**
In-Sync Replicas — followers caught up within the allowed lag. With `acks=all` + `min.insync.replicas=2`, writes are only acknowledged (or fail loudly) when the ISR is healthy enough to survive a broker loss.

**10. Explain `acks` values.**
`0` fire-and-forget (fastest, lossy); `1` leader-only ack (lost if leader dies pre-replication); `all` full ISR ack (most durable, slower).

**11. What happens when a consumer in a group dies?**
Missed heartbeats past `session.timeout.ms` → the coordinator triggers a rebalance → its partitions are redistributed → survivors resume from committed offsets. Uncommitted work is redelivered.

**12. At-least-once vs at-most-once — what decides it?**
Commit timing. Commit after processing → at-least-once (duplicates possible); commit before processing → at-most-once (loss possible). Production systems choose at-least-once plus idempotent handlers.

**13. What is the idempotent producer for?**
Retries can duplicate a message when an ack is lost. `enable.idempotence` gives the broker producer-ID + sequence numbers to de-duplicate, making retries safe.

**14. ZooKeeper vs KRaft?**
ZooKeeper was Kafka's external metadata store (controller election, broker registry, configs). KRaft removes it: controllers run Raft among themselves, metadata is a replicated log, scaling to millions of partitions with one fewer system to operate. ZooKeeper support was removed in Kafka 4.0.

**15. What is retention? Can consumers replay?**
Per-topic policy deleting the oldest log segments by time (`retention.ms`, default 7d) or size (`retention.bytes`). Yes — data stays after consumption, so any group can rewind to any offset within retention.

**16. How do you handle a message that keeps failing?**
Retry-topic pattern with an attempt counter and backoff; after N attempts, publish to a dead-letter topic for inspection and replay. Deterministic failures (poison payloads) go straight to the DLQ.

## Advanced

**17. How would you fix a hot partition?**
Three options: drop the key (if no ordering needed); compound key to shard one entity across k partitions (`adId:1..k`, order per shard, downstream merges); or producer backpressure. Root cause is key/cardinality choice.

**18. What breaks when you add partitions to an existing topic?**
Key→partition mapping changes (modulo N changes), so a key's events split across the old and new partitions — per-key ordering breaks at the boundary, and consumers may see a brief overlap. Plan partition counts up front; grow early and deliberately.

**19. How do you get exactly-once?**
Kafka-to-Kafka: idempotent producer + transactions (offsets committed atomically with outputs; Kafka Streams offers this as EOS). Anything leaving Kafka: exactly-once is impossible end-to-end — use at-least-once plus idempotent effects.

**20. A consumer group was offline longer than retention. What happens?**
Its committed offsets may point to deleted segments; on restart the `auto.offset.reset` policy (`earliest`/`latest`) decides where it resumes — usually a silent gap. Monitor lag and size retention to outages.

**21. How do you monitor Kafka health?**
Consumer lag first (the customer-facing metric), then under-replicated partitions, active controller count/offline partitions, request latency/purgatory, disk vs retention budget, rebalance rate, DLQ depth.

**22. What is log compaction and when do you use it?**
`cleanup.policy=compact` keeps at least the latest record per key, pruning older versions — turning a topic into a key→value table (configs, changelogs, materialized state) instead of a time-windowed buffer.

**23. Why not put large messages (video blobs) on Kafka?**
Throughput, memory, and replication strain; keep messages under ~1 MB. Store the blob in object storage and publish a pointer (claim-check pattern).

**24. When would you choose RabbitMQ over Kafka?**
Complex per-message routing (topic exchanges, TTL, priorities, delays), classic task queues with destructive consumption, request/reply, modest throughput, no replay needs. Kafka when you need replay, retention, multi-reader streams, or very high throughput.

**25. Design a click analytics pipeline (classic).**
Clicks topic keyed by ad_id (watch the hot-ad case) → stream processor aggregates windows (per ad_id, per shard) → aggregates sink to a DB for dashboards; raw topic retained for replay/reprocessing; hot ads handled via compound keys with per-shard merge.

---

Done with the guide? Re-read [Part 2 chapter 06](../part-2-kafka-deep-dive/06-the-big-picture.md) and redraw the architecture diagram from memory — if you can, you own this material.
