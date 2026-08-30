# 11 — Deep Dive: Performance & Throughput

> Level: Advanced

When Kafka is a *stream* (not just a queue), consumers must keep up with production. Two producer-side tools plus one architectural decision dominate throughput.

---

## 1. Batching — fewer, bigger requests

The producer can accumulate messages and send them as one request:

| Config | Meaning |
|---|---|
| `batch.size` (bytes) | Max batch size — a batch is sent once it fills |
| `linger.ms` | Max time to wait for more messages before sending a partial batch |

Either threshold trips → one network request carries many messages. Throughput rises dramatically at the cost of a few milliseconds of latency. Tune `linger.ms` (e.g., 5–100 ms) to the latency budget of your use case.

## 2. Compression — fewer bytes on the wire

The producer can compress batches:

```
compression.type = none | gzip | snappy | lz4 | zstd
```

Smaller batches → less network and broker disk I/O. Compression costs producer CPU; `lz4`/`zstd` usually win the ratio/speed trade-off for JSON-ish payloads. Because compression applies *per batch*, batching and compression compound each other.

## 3. Partitioning — the real throughput lever

The biggest impact is (again) the **partition key + partition count**:

- Parallelism = partitions spread evenly across brokers, each with its own producer write path and consumer reader
- An even key distribution maximizes useful parallelism; one hot partition throttles the whole pipeline back to single-partition speed (see [08 — Scalability](08-scalability.md))

**In any performance discussion, the partitioning strategy is where you start** — batching and compression are refinements; parallelism is the architecture.

## 4. Why Kafka is fast (honorable mentions)

Worth knowing for interviews; you don't tune these directly:

- **Sequential disk I/O** — append-only logs make disks behave like tape drives: predictable, fast writes
- **Zero-copy transfer** — bytes flow disk → network card without copying through application memory (`sendfile`)
- **Page cache reliance** — Kafka caches nothing itself; it leans on the OS page cache, so consumers reading recent data hit RAM
- **Batches everywhere** — network, storage, and client APIs are all batch-shaped

---

## Throughput tuning checklist (ordered by impact)

1. Partition count & key distribution — is anything hot? Are consumers maxed at partition count?
2. Batching: `linger.ms`, `batch.size`
3. Compression: `lz4`/`zstd`
4. Consumer fetch sizes and parallel processing per partition
5. Hardware/network (and managed-service sizing)

**Next:** [12 — Deep dive: retention and replay](12-retention.md)
