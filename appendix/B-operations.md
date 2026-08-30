# Appendix B — Operations: Capacity, Monitoring, Failure Scenarios

> Running Kafka in production: planning decisions, the metrics that matter, and the failures you will actually meet.

---

## 1. Partitioning strategy (the planning decision)

**Choosing partition count** — the ceilings to reconcile:

- Consumer throughput ceiling: max parallel consumers per group = partition count
- Broker I/O ceiling: aim for a balanced spread; more partitions = more open files, more leader elections to orchestrate
- Throughput per partition is bounded (~10s of MB/s); total throughput ceiling ≈ partitions × per-partition rate

Rules of thumb:

1. Start from *target throughput ÷ achievable per-partition throughput*, then round up with headroom
2. Key ordering scope decides the key; the key decides evenness — verify with real traffic distributions
3. **Partitions only grow.** Adding partitions later rehashes `murmur2(key) % N`, so a given key may move partitions: per-key ordering breaks across the boundary (old events behind you, new events ahead elsewhere). Underestimate deliberately and grow once, early, on a schedule you control — or key carefully and accept the boundary.

**Capacity quick-math:** brokers needed ≈ (ingress MB/s × retention) ÷ (storage per broker, ~1 TB) — then check each broker stays under ~10k msg/s-class load for its message sizes.

## 2. Monitoring — the metric hierarchy

| Priority | Metric | Source | Why |
|---|---|---|---|
| 1 | **Consumer group lag** | `kafka-consumer-groups`, Burrow, client APIs ([Part 3 ch. 05](../part-3-go-and-configs/05-admin-and-observability.md)) | THE customer-facing number: is anything falling behind? |
| 2 | **Under-replicated partitions** | JMX `kafka.server:type=ReplicaManager` | Durability degraded — followers lagging |
| 3 | **Active controller count / offline partitions** | JMX | Cluster sanity (must be exactly 1 controller; 0 offline) |
| 4 | **Request latency + time on request purgatory** | JMX | Producer `acks=all` stalls show here |
| 5 | **Disk usage vs retention budget** | node metrics | Retention × throughput fills disks on schedule |
| 6 | Rebalance rate/frequency | group coordinator metrics | Rebalance storms destroy throughput |
| 7 | DLQ depth | your tooling | Every message there is a defect |

JMX is the standard export path (Prometheus JMX exporter → Grafana; MSK/Confluent expose managed dashboards).

## 3. Throughput tuning (recap, ordered by leverage)

1. Fix hot partitions (key choice, compound keys — [Part 2 ch. 08](../part-2-kafka-deep-dive/08-scalability.md))
2. Producer batching (`linger.ms`, `batch.size`) + compression (`lz4`/`zstd`)
3. `acks` per durability requirement — don't pay `all` for droppable telemetry
4. Consumer fetch sizes, parallelism to partition count
5. Then hardware/network/managed-service sizing

## 4. Common failure scenarios (symptom → cause → cure)

| Symptom | Likely cause | Cure |
|---|---|---|
| One partition's lag grows; others flat | Hot key | Compound/shard the key, or re-key (ch. 08) |
| Whole group's lag grows together | Slow handler or too few partitions | Profile handler; check consumers = partitions; raise parallelism |
| Lag grows although processing looks idle | Consumer actually dead (no heartbeats), offsets frozen | Restart/fix crash loop; monitor "last processed" timestamps |
| Processing gaps after long downtime | Group offsets expired past retention → reset policy kicked in | `auto.offset.reset=earliest` + dedup; raise `offsets.retention.minutes` |
| Producer errors: NOT_ENOUGH_REPLICAS | ISR shrank below `min.insync.replicas` | Heal/replace broker; check disk/network; don't "fix" by lowering min ISR silently |
| Constant rebalances | `session.timeout` vs long processing; flaky members | Tune timeouts / `max.poll`-style batch sizes; keep handlers fast |
| Broker disk full | Retention × ingress exceeded plan | Raise brokers/storage; audit retention & compaction; check for giant messages |
| Ghost topics with 1 partition | Auto-create + typo'd topic name | `auto.create.topics.enable=false`, explicit creation (Part 3 ch. 05) |
| CLI shows messages, consumer silent | Subscribed to wrong/nonexistent topic name (failure is *quiet*) | Explicit topics + alerting on zero-consumption |

## 5. Operational checklist (a sane baseline)

- [ ] RF=3, `min.insync.replicas=2`, producers `acks=all` + idempotence
- [ ] Auto-create off; topics created via IaC/admin code
- [ ] Lag alerting on every group; DLQ depth alerting on every pipeline
- [ ] Retention sized to a storage budget, compaction where state-like
- [ ] Controlled partition growth plan (grow early, once)
- [ ] Broker upgrades/loss rehearsed (leader elections, ISR recovery)

**Next:** [Appendix C — hands-on project](C-hands-on-project.md)
