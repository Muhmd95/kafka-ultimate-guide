# 04 — Delivery Semantics: At-Most-Once, At-Least-Once, Exactly-Once

> Level: Advanced

This is where everything converges: acks, retries, idempotence, commit timing. Kafka's guarantees are not a switch you flip — they are **the sum of your config + code choices** on both sides.

---

## The three semantics

| Semantics | Guarantee | Achieved by | Failure outcome |
|---|---|---|---|
| **At-most-once** | Message processed 0 or 1 times | Commit offset *before* processing | Crash → message skipped (possible loss) |
| **At-least-once** | Message processed 1+ times | Commit offset *after* processing | Crash in the window → redelivery (possible duplicates) |
| **Exactly-once** | Processed exactly 1 time | Idempotent producer + transactions (Kafka→Kafka) / idempotent handler (any sink) | Neither |

## 1. At-most-once (rarely what you want)

```go
func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "") // mark FIRST
		processAsync(msg)          // crash here = message never revisited
	}
	return nil
}
```

Acceptable for droppable telemetry (metrics, sampled logs) where duplicates are worse than gaps.

## 2. At-least-once + the idempotent consumer (the workhorse pattern)

```go
func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := processIdempotent(msg); err != nil {
			// bounded retries → skip-and-log / retry topic / DLQ (Part 2 ch. 10)
		}
		sess.MarkMessage(msg, "") // mark AFTER the work settles
	}
	return nil
}
```

Redelivery happens — count on it (autocommit flush gap, rebalances, crashes). What makes it safe is **idempotency at the point of effect**:

```go
// the pattern: a unique key + a duplicate-tolerant write
func processIdempotent(msg *sarama.ConsumerMessage) error {
	event := decode(msg)
	_, err := db.Exec(`INSERT INTO notifications (txn_id, payload)
	                   VALUES ($1, $2)
	                   ON CONFLICT (txn_id) DO NOTHING`, // unique index on txn_id
		event.TxnID, event.Payload)
	return err
}
```

Unique constraint (or `INSERT ... ON CONFLICT DO NOTHING`, or upserts, or dedup tables) turns "at-least-once delivery" into **"effectively-once processing"**. This combo — *commit after settle + idempotent sink* — is what most production systems actually run.

## 3. Exactly-once, honestly

Kafka's transactions (`transactional.id`, `enable.idempotence`, transactional send + `sendOffsetsToTransaction`) give **exactly-once Kafka-to-Kafka**: a stream processor that consumes topic A and produces topic B commits input offsets *inside* the same transaction as its outputs — atomic across the whole pipeline (Kafka Streams uses this for its EOS guarantees).

But **the moment your effect leaves Kafka** (write to a DB, call an API, send an email), transactions can't cover it — no coordinator spans Kafka + Mongo + SMTP. The honest menu:

- Kafka→Kafka pipeline → real exactly-once via transactions
- Anything else → **at-least-once + idempotent effects** (§2) — which is exactly-once *as observed by the world*

```go
// producer side: idempotent + transactional (Kafka→Kafka EOS)
config.Producer.Idempotent = true
config.Producer.Transaction.ID = "processor-1"
```

sarama exposes transactions (`InitTxSession`, `BeginTx`...); the vast majority of Go services are better served by the §2 pattern.

## 4. The cheat sheet

| Choice | Where | Result |
|---|---|---|
| `Idempotent = true`, `Retry.Max > 0`, `WaitForAll` | Producer | No duplicate *logs*, safe retries |
| Mark → process | Consumer | At-most-once |
| Process → mark | Consumer | At-least-once |
| Unique key / upsert at the sink | Handler | Duplicates become no-ops |
| `AutoCommit.Interval` | Consumer | Width of the replay window |
| Transactions | Both | EOS within Kafka's borders |

> The one-sentence interview answer: *"Delivery semantics are decided by commit timing relative to side effects; duplicates are unavoidable in the window, so production systems make effects idempotent and get exactly-once behavior by construction."*

**Next:** [05 — admin: topics, offsets, and lag from Go](05-admin-and-observability.md)
