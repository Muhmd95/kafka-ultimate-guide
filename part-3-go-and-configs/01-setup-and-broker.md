# 01 — Setup: Local Broker (KRaft) + First Connection

> Level: Intermediate

Goal of this chapter: a **single-node KRaft Kafka broker in Docker**, reachable from both containers and your host machine, plus the sarama basics every program starts with.

---

## 1. The broker: docker-compose

```yaml
# docker-compose.yml
services:
  kafka:
    image: confluentinc/cp-kafka:7.5.16
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      # --- identity (KRaft) ---
      KAFKA_NODE_ID: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"   # any 22-char base64 UUID
      KAFKA_PROCESS_ROLES: broker,controller            # one node, both hats
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093     # the quorum talks to itself
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # --- networking: two doors ---
      # PLAINTEXT       → for containers (advertised as kafka:29092)
      # PLAINTEXT_HOST  → for your machine (advertised as localhost:9092)
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      # --- single-node realities ---
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0        # snappier dev rebalances
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list"]
      interval: 10s
      timeout: 10s
      retries: 5

volumes:
  kafka-data:
```

**What to understand, line by line:**

| Config | Why it exists |
|---|---|
| `PROCESS_ROLES: broker,controller` | KRaft mode: this node both stores data *and* participates in the metadata quorum (no ZooKeeper) |
| `CONTROLLER_QUORUM_VOTERS` | The Raft voter set: `nodeId@host:port`. One node votes with itself |
| `LISTENERS` vs `ADVERTISED_LISTENERS` | A listener is where the broker *listens*; the advertised listener is the *business card* it hands clients for subsequent connections. Containers must dial `kafka:29092` (Docker DNS), your laptop dials `localhost:9092` |
| `*_REPLICATION_FACTOR: 1` | One broker can only hold one copy — production clusters use 3 |
| healthcheck `kafka-topics --list` | The broker is "ready" only when it answers metadata queries (~15–30s after start) |

```bash
docker compose up -d          # start
docker compose ps             # wait for (healthy)
```

## 2. CLI toolbox (sanity checks you will use forever)

```bash
# create a topic explicitly
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

# describe it: partition count, leader per partition
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic orders

# produce from the CLI
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 \
  --topic orders --property parse.key=true --property key.separator=:

# consume everything, with location stamps
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true

# consumer group progress (LAG tells the downtime story)
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group
```

## 3. sarama basics

```bash
mkdir kafka-demo && cd kafka-demo
go mod init kafka-demo
go get github.com/IBM/sarama
```

Every sarama program starts with a config and an address list:

```go
package main

import (
	"fmt"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"} // PLAINTEXT_HOST listener

	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0 // pin the protocol version; raise for newer brokers

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	brokersMeta := client.Brokers()
	for _, b := range brokersMeta {
		fmt.Println("connected to broker:", b.Addr())
	}
}
```

**The configs that matter here:**

| Config | Syntax | Meaning |
|---|---|---|
| `config.Version` | `sarama.V3_5_0_0` | Protocol features negotiated with the broker. Pin it explicitly — sarama's `DefaultVersion` is 2.8.0, so newer broker features need an explicit raise. Constants are 4-segment (`V2_8_0_0`, `V3_5_0_0`, ...) up to the newest supported release |
| `config.Net.DialTimeout` | `30 * time.Second` | TCP connect timeout |
| `config.Net.ReadTimeout` / `WriteTimeout` | `30 * time.Second` | Socket read/write timeouts |
| `config.ClientID` | `"orders-api"` | Identifies your app in broker logs/debugging |

> **Bootstrap ≠ everything:** the client connects to one broker just to fetch *metadata* (which broker leads which partition), then talks to the correct brokers directly. That's why advertised listeners must be reachable from *where your code runs* — the business-card problem from §1.

**Next:** [02 — the producer, and the configs that shape durability & throughput](02-producer.md)
