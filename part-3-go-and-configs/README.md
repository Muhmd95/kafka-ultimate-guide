# Part 3 — Kafka with Go: Code, Configs, and What They Mean

> Client library: [`github.com/IBM/sarama`](https://github.com/IBM/sarama) — the battle-tested Go client for Kafka.

This part turns every Part 2 concept into runnable Go, with the **most important configs** explained: what each one does, its syntax, and when to change it. All snippets are self-contained and run against the local broker from chapter 01 (runnable copies live in [`examples/`](../examples/)).

| # | Chapter | Level |
|---|---------|-------|
| 01 | [Setup: Local Broker (KRaft) + First Connection](01-setup-and-broker.md) | Intermediate |
| 02 | [Producer in Go — Code & Configs](02-producer.md) | Intermediate |
| 03 | [Consumer Groups in Go — Code & Configs](03-consumer.md) | Intermediate |
| 04 | [Delivery Semantics: At-Most/At-Least/Exactly-Once](04-delivery-semantics.md) | Advanced |
| 05 | [Admin & Observability: Topics, Offsets, Lag](05-admin-and-observability.md) | Intermediate |

**Reading order:** strictly sequential — each chapter builds on the previous. Chapter 04 is the conceptual summit of the whole guide; everything before it exists to make it click.
