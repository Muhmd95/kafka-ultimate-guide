# 06 — The Big Picture: Full Architecture

> Level: Beginner

Everything from chapters 01–05 assembled into one diagram. If you can redraw and narrate this, you understand Kafka's architecture.

```mermaid
flowchart TB
    P1["Producer A<br/>writes to topic X"]
    P2["Producer B<br/>writes to topic Y"]
    subgraph CLUSTER["Kafka cluster (KRaft)"]
        direction TB
        CTRL["Controller<br/>metadata, leader election"]
        subgraph BK1["Broker 1"]
            X0["topic X · p0 (leader)"]
            X2["topic X · p2 (leader)"]
            Y1["topic Y · p1 (leader)"]
        end
        subgraph BK2["Broker 2"]
            X0F["topic X · p0 (follower)"]
            X1["topic X · p1 (leader)"]
            Y0["topic Y · p0 (leader)"]
        end
        subgraph BK3["Broker 3"]
            X2F["topic X · p2 (follower)"]
            X1F["topic X · p1 (follower)"]
            Y1F["topic Y · p1 (follower)"]
        end
    end
    CTRL --- BK1
    CTRL --- BK2
    CTRL --- BK3
    P1 -- "producer API" --> X0
    P1 --> X1
    P1 --> X2
    P2 -- "producer API" --> Y0
    P2 --> Y1
    subgraph CG1["Consumer group 1 (topic X)"]
        C1["member 1"]
        C2["member 2"]
    end
    subgraph CG2["Consumer group 2 (topic X)"]
        C3["member 1<br/>(independent reader)"]
    end
    X0 -- "consumer API" --> C1
    X2 --> C2
    X1 --> C3
```

## Narrating the diagram

1. **Producers** write records to topics via the **producer API**. Each record's **key** is hashed (`murmur2(key) % partitions`) to pick a partition; metadata tells the producer which broker leads that partition, and the write goes straight to that leader.
2. **Brokers** are the servers. Each **partition** is an append-only log on disk; each partition has a **leader** (serves traffic) and **followers** (replicate). The **controller** keeps assignments balanced and handles leader elections. In KRaft mode, controllers manage metadata via Raft with no ZooKeeper.
3. **Topics** are logical groupings: topic X's partitions are scattered over brokers; a consumer subscribing to topic X reads all its partitions.
4. **Consumers** in the same **group** split the partitions among themselves (one partition → exactly one member); **different groups** each read the whole stream independently.
5. Consumers **pull** batches, process, and periodically **commit offsets** to Kafka (`__consumer_offsets`) — restarts and rebalances resume from those commits.

## One-table recap

| Concept | One-liner |
|---|---|
| Producer | Writes records to a topic, key decides partition |
| Broker | Server storing partition logs |
| Controller | Metadata authority; assigns leaders |
| Topic | Logical stream name |
| Partition | Physical append-only log; unit of ordering & parallelism |
| Offset | Record position; consumer's cursor |
| Consumer group | Exclusive per-partition readers; the work-queue mechanism |
| Replication | Leader + follower copies; durability under broker loss |
| KRaft | Kafka's built-in Raft metadata mode (no ZooKeeper) |

**Next:** [07 — when you should actually reach for Kafka](07-when-to-use-kafka.md)
