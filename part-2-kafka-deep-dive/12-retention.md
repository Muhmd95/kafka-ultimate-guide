# 12 — Deep Dive: Retention & Replay

> Level: Advanced

Unlike queue brokers that delete on consumption, Kafka **retains** messages after they are consumed. Retention policy — configured **per topic** — decides how long the log keeps them.

---

## 1. The two retention settings

| Setting | Meaning | Default |
|---|---|---|
| `retention.ms` | Maximum age of a message before deletion | 7 days |
| `retention.bytes` | Maximum size of a partition's log before oldest segments are dropped | `-1` (unlimited) — commonly set, e.g., 1 GB per partition |

**Whichever limit triggers first wins.** As the log ages past `retention.ms`, or grows past `retention.bytes`, the broker deletes the *oldest log segments* (deletion is segment-granular, not message-granular).

## 2. Why retention matters: replay

Retained data = **rewindable history**:

- A consumer that fell behind (downtime, bug, slow processing) catches up from its committed offsets
- A **new consumer group** can replay the entire topic from offset 0 — rebuild a view, backfill a database, re-run fixed logic
- Time-travel debugging: reproduce a bug by re-processing the exact input stream

If a design needs "reprocess last month of events" (event-sourced views, model retraining, audit), size `retention.ms` accordingly — and call out the **storage cost and broker load** that implies: retention × throughput = storage budget.

## 3. Log compaction (the third option)

For *latest-value-per-key* topics (config state, changelogs, a materialized table), **compaction** replaces `retention.ms`: instead of deleting by age, Kafka keeps **at least the last record for every key**, pruning older records for the same key in the background.

```
key=p1: v1, v2, v3      →  compacted: p1:v3 (latest survives)
key=p2: v1              →  kept:      p2:v1
```

A compacted topic is an infinite stream that *behaves like a table*. Combine `cleanup.policy=compact` with `delete` for hybrid policies.

## 4. Choosing retention in practice

| Use case | Policy |
|---|---|
| High-volume telemetry, replayable 1–3 days | `retention.ms` small |
| Standard eventing, week of recovery | Defaults (7 days) |
| Audit / reprocessing / event sourcing | Long `retention.ms`, justify storage |
| Latest-state topics (configs, caches) | `cleanup.policy=compact` |

One caveat to internalize early: **consumers can outlive retention.** If a consumer group is down longer than the retention period, its committed offsets may point at deleted data — on restart it hits `auto.offset.reset` (`earliest`/`latest`) policy instead. Long outages + short retention = silent gaps; monitor group lag (see [Appendix B](../appendix/B-operations.md)).

---

## Key takeaways

1. Retention is per-topic: time (`retention.ms`), size (`retention.bytes`), whichever first; compaction is the key-based third mode.
2. Retention buys **replay** — the superpower queue brokers don't have.
3. Retention × throughput = storage budget; state it explicitly in designs.
4. Offsets can expire inside the retention window of *your group being offline* — lag monitoring is the safety net.

This closes Part 2. Continue with [Part 3 — Go code and the configs that matter](../part-3-go-and-configs/README.md), or jump to the [appendices](../appendix/).
