# 10 — Deep Dive: Errors, Retries, and Dead-Letter Queues

> Level: Advanced

Kafka itself is reliable — the fragile parts are *getting messages into* and *doing work after getting messages out of* the cluster. Both sides need deliberate retry policies.

---

## 1. Producer side: retries and idempotence

A producer send can fail on network blips or broker unavailability (e.g., a leader election in progress). Producer configs handle this natively:

| Config | Role |
|---|---|
| `retries` / `retry.backoff.ms` | How many times and how eagerly to retry a failed send (libraries also offer exponential backoff) |
| `enable.idempotence` | **Prevents duplicates caused by retries** — the broker de-duplicates by producer ID + sequence number |

The retry loop is conceptually trivial: *keep trying until the broker acknowledges receipt.* The subtle failure is the **lost ack**: the broker stored the message but the acknowledgment never reached the producer → the producer retries → **duplicate** in the log. With `acks=all` you will (and should) enable the idempotent producer, which makes retries safe: each logical send is stored once even across re-sends.

> Go/sarama specifics for every setting above: [Part 3 chapter 02](../part-3-go-and-configs/02-producer.md).

## 2. Consumer side: the retry-topic pattern

Here is the asymmetry that surprises people: **Kafka has no built-in consumer retry.** (SQS, notably, ships consumer redelivery/DLQ out of the box — a fair point when comparing technologies.) If your *handler* fails — an upstream API is down, a dependency times out — Kafka will not redeliver automatically; blocking the partition to retry forever is a bug, not a strategy.

The standard pattern adds two topics:

```mermaid
flowchart LR
    MT["main topic"] --> C["consumer"]
    C -- "fails, count < N" --> RT["retry topic<br/>(message carries retry count)"]
    RT --> C2["retry consumer<br/>(waits / slower pace)"]
    C2 -- "still failing, count = N" --> DLQ["dead-letter topic"]
    DLQ --> ENG["engineers inspect & replay"]
```

Step by step:

1. Consumer pulls a message, attempts the work. It fails.
2. Producer-publish the message to a **retry topic**, stamping it with a retry count (in the payload or a header)
3. The same or another consumer reads the retry topic — optionally with delay/backoff pacing — and attempts again, incrementing the count
4. After **N** attempts, publish to the **dead-letter queue** (a normal topic nobody auto-consumes), where messages persist for inspection
5. Engineers examine the DLQ, fix root causes, and **replay** messages back into the pipeline

**Design notes:**

- Distinguish *retryable* failures (network timeouts, 503s — time may heal them) from *deterministic* failures (malformed payload — time never heals them; route those straight to the DLQ)
- A DLQ is not an apology box: monitor its depth, alert on growth, and treat every message there as a defect to triage
- This whole pattern is exactly what Kafka Connect gives you config-only (`errors.tolerance`, `errors.deadletterqueue.topic.name`, ...) — see [Appendix A](../appendix/A-kafka-ecosystem.md)

## 3. The failure taxonomy (putting it together)

| Failure | Who handles it | Tool |
|---|---|---|
| Send fails / ack lost | Producer client | `retries` + idempotent producer |
| Broker dies | Cluster | Replication, leader election, ISR |
| Handler fails transiently | Your consumer | Retry topic + backoff |
| Handler fails permanently | Your consumer | DLQ + monitoring + replay |
| Consumer crashes mid-work | Group machinery | Rebalance + offset replay (at-least-once) + idempotent handlers |

---

## Key takeaways

1. Producer retries are built-in; **idempotence makes them safe** — enable it.
2. Consumer retries are **your** job — the retry-topic pattern adds bounded attempts with pacing, then a DLQ.
3. Separate transient from deterministic failures; never retry poison payloads.
4. DLQs must be monitored and replayed — otherwise they are just polite data loss.

**Next:** [11 — Deep dive: performance and throughput](11-performance.md)
