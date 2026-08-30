# Part 2 — The Kafka Deep Dive

This part derives Kafka from first principles, then goes deep. It follows a deliberate order: start with a motivating example that *forces* Kafka's design to exist, formalize the terminology, follow a message's full lifecycle, look at replication and cluster coordination, tie everything into one architecture picture, and then study the five deep-dive areas where Kafka knowledge is really tested: **scalability, fault tolerance & durability, errors & retries, performance, and retention**.

> **Credit:** the structure and core narrative of chapters 01–12 are adapted from the talk *"Kafka System Design Deep Dive"* by Evan (ex-Meta Staff Engineer) of [hello interview](https://www.hellointerview.com) — an excellent companion resource.

| # | Chapter | Level |
|---|---------|-------|
| 01 | [From One Queue to Kafka — The Motivating Example](01-from-queue-to-kafka.md) | Beginner |
| 02 | [Core Concepts — Brokers, Topics, Partitions, Producers, Consumers](02-core-concepts.md) | Beginner |
| 03 | [Anatomy of a Message & The Message Lifecycle](03-message-lifecycle.md) | Beginner |
| 04 | [Replication — Leaders, Followers, and Durability](04-replication.md) | Intermediate |
| 05 | [Cluster Coordination — ZooKeeper vs KRaft](05-zookeeper-vs-kraft.md) | Intermediate |
| 06 | [The Big Picture — Full Architecture](06-the-big-picture.md) | Beginner |
| 07 | [When to Use Kafka — Queue & Stream Use Cases](07-when-to-use-kafka.md) | Beginner |
| 08 | [Deep Dive: Scalability & Hot Partitions](08-scalability.md) | Intermediate |
| 09 | [Deep Dive: Fault Tolerance & Durability](09-fault-tolerance-and-durability.md) | Intermediate |
| 10 | [Deep Dive: Errors & Retries, DLQs](10-errors-and-retries.md) | Advanced |
| 11 | [Deep Dive: Performance & Throughput](11-performance.md) | Advanced |
| 12 | [Deep Dive: Retention & Replay](12-retention.md) | Advanced |
