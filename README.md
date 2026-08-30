# The Kafka Ultimate Guide

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A complete, gradual Kafka guide — from *"why do message brokers exist"* all the way to delivery semantics, cluster coordination, and interview-ready depth. Concepts are explained with diagrams; every Go-facing chapter ships runnable code.

Built as three parts plus appendices:

- **Part 1 — Foundations**: message brokers, the system designs that need them, and the RabbitMQ-vs-Kafka decision
- **Part 2 — The Deep Dive**: Kafka derived from first principles, then the five areas where knowledge is really tested (scalability, fault tolerance, errors/retries, performance, retention)
- **Part 3 — Go & Configs**: sarama code for producer/consumer/admin, and every important config explained with syntax and trade-offs

---

## Table of Contents

### Part 1 — Foundations
| # | Chapter | Level |
|---|---------|-------|
| 1.1 | [Message Brokers & The Systems That Need Them](part-1-foundations/01-message-brokers-and-system-design.md) | Beginner |
| 1.2 | [RabbitMQ vs Kafka](part-1-foundations/02-rabbitmq-vs-kafka.md) | Beginner |

### Part 2 — The Kafka Deep Dive
| # | Chapter | Level |
|---|---------|-------|
| 2.1 | [From One Queue to Kafka — The Motivating Example](part-2-kafka-deep-dive/01-from-queue-to-kafka.md) | Beginner |
| 2.2 | [Core Concepts — Brokers, Topics, Partitions, Producers, Consumers](part-2-kafka-deep-dive/02-core-concepts.md) | Beginner |
| 2.3 | [Anatomy of a Message & The Message Lifecycle](part-2-kafka-deep-dive/03-message-lifecycle.md) | Beginner |
| 2.4 | [Replication — Leaders, Followers, and Durability](part-2-kafka-deep-dive/04-replication.md) | Intermediate |
| 2.5 | [Cluster Coordination — ZooKeeper vs KRaft](part-2-kafka-deep-dive/05-zookeeper-vs-kraft.md) | Intermediate |
| 2.6 | [The Big Picture — Full Architecture](part-2-kafka-deep-dive/06-the-big-picture.md) | Beginner |
| 2.7 | [When to Use Kafka — Queue & Stream Use Cases](part-2-kafka-deep-dive/07-when-to-use-kafka.md) | Beginner |
| 2.8 | [Deep Dive: Scalability & Hot Partitions](part-2-kafka-deep-dive/08-scalability.md) | Intermediate |
| 2.9 | [Deep Dive: Fault Tolerance & Durability](part-2-kafka-deep-dive/09-fault-tolerance-and-durability.md) | Intermediate |
| 2.10 | [Deep Dive: Errors & Retries, DLQs](part-2-kafka-deep-dive/10-errors-and-retries.md) | Advanced |
| 2.11 | [Deep Dive: Performance & Throughput](part-2-kafka-deep-dive/11-performance.md) | Advanced |
| 2.12 | [Deep Dive: Retention & Replay](part-2-kafka-deep-dive/12-retention.md) | Advanced |

### Part 3 — Kafka with Go
| # | Chapter | Level |
|---|---------|-------|
| 3.1 | [Setup: Local Broker (KRaft) + First Connection](part-3-go-and-configs/01-setup-and-broker.md) | Intermediate |
| 3.2 | [Producer in Go — Code & Configs](part-3-go-and-configs/02-producer.md) | Intermediate |
| 3.3 | [Consumer Groups in Go — Code & Configs](part-3-go-and-configs/03-consumer.md) | Intermediate |
| 3.4 | [Delivery Semantics: At-Most/At-Least/Exactly-Once](part-3-go-and-configs/04-delivery-semantics.md) | Advanced |
| 3.5 | [Admin & Observability: Topics, Offsets, Lag](part-3-go-and-configs/05-admin-and-observability.md) | Intermediate |

### Appendices
| # | Chapter |
|---|---------|
| A | [The Kafka Ecosystem — Connect, Streams, ksqlDB, Schema Registry](appendix/A-kafka-ecosystem.md) |
| B | [Operations — Capacity, Monitoring, Failure Scenarios](appendix/B-operations.md) |
| C | [Hands-On Project — An Order-Processing Pipeline](appendix/C-hands-on-project.md) |
| D | [Interview Questions & Answers](appendix/D-interview-questions.md) |

### Runnable code
| Path | Contents |
|------|----------|
| [`examples/`](examples/) | Producer, consumer, and admin programs from Part 3 |
| [`hands-on/`](hands-on/) | `docker-compose.yml` for a local KRaft broker |

---

## Suggested reading paths

- **Complete beginner:** read straight through — the order is deliberate (each chapter's difficulty is labeled).
- **Refreshing for an interview:** 2.1 → 2.2 → 2.6 → Appendix D, then chase links into whatever feels shaky.
- **Hands-on learner:** Part 1 → 2.1–2.3 → all of Part 3 → Appendix C project.

## Credits

- The structure and core narrative of Part 2 are adapted from the talk **"Kafka System Design Deep Dive"** by Evan (ex-Meta Staff Engineer) of [hello interview](https://www.hellointerview.com) — including their excellent written deep dives. If you are preparing for system design interviews, their free content is superb.
- Go examples use [`github.com/IBM/sarama`](https://github.com/IBM/sarama).

## License

Released under the [MIT License](LICENSE).
