# 02 — Producer in Go: Code & Configs

> Level: Intermediate

The producer chapter covers both personalities — **sync** (simple, fail-visible) and **async** (high-throughput, more obligations) — then every important knob: acks, retries, idempotence, batching, compression.

---

## 1. The synchronous producer

`SyncProducer.SendMessage` blocks until the broker acknowledges (or the attempt fails). Perfect when a publish is part of a request you want to fail visibly.

```go
package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0
	config.Producer.Return.Successes = true // REQUIRED for the sync producer

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "orders",
		Key:   sarama.StringEncoder("order-123"), // same key -> same partition -> order kept
		Value: sarama.ByteEncoder(`{"order_id":"order-123","amount":42}`),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("publish failed: %v", err)
		return
	}
	fmt.Printf("stored partition=%d offset=%d\n", partition, offset)
}
```

`Return.Successes = true` is the classic sarama tripwire: without it, `NewSyncProducer` refuses to start — the sync API needs the success channel populated.

## 2. The async producer

For pipelines pushing thousands of messages per second, the async producer takes a channel and batches internally:

```go
producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
if err != nil {
	log.Fatal(err)
}

// OBLIGATION: drain both channels or the producer deadlocks / failures vanish
go func() {
	for err := range producer.Errors() {
		log.Printf("async publish failed: %v", err)
	}
}()
go func() {
	for m := range producer.Successes() {
		_ = m // usually just metrics; keep the drain regardless
	}
}()

producer.Input() <- &sarama.ProducerMessage{
	Topic: "orders",
	Key:   sarama.StringEncoder("order-123"),
	Value: sarama.ByteEncoder(`{"order_id":"order-123"}`),
}

defer producer.AsyncClose() // flushes in-flight messages, then closes channels
```

**Rule of thumb:** choose sync unless you have a reason not to. Async's throughput is real, but you own the draining goroutines and the graceful shutdown — obligations sync simply doesn't have.

## 3. The configs that matter

### 3.1 Durability

| Config | Syntax | Meaning |
|---|---|---|
| `RequiredAcks` | `sarama.NoResponse` / `sarama.WaitForLocal` / `sarama.WaitForAll` | Kafka's `acks=0 / 1 / all`. `WaitForAll` = every in-sync replica confirms — the durable choice ([Part 2 ch. 09](../part-2-kafka-deep-dive/09-fault-tolerance-and-durability.md)) |
| `Producer.Timeout` | `5 * time.Second` | How long to wait for those acks before the attempt fails |

```go
config.Producer.RequiredAcks = sarama.WaitForAll // acks=all
config.Producer.Timeout = 5 * time.Second
```

### 3.2 Retries & idempotence

| Config | Syntax | Meaning |
|---|---|---|
| `Producer.Retry.Max` | `5` | Send attempts on transient failures (leader elections, network blips) |
| `Producer.Retry.Backoff` | `100 * time.Millisecond` | Pause between attempts (use `BackoffFunc` for exponential) |
| `Producer.Idempotent` | `true` | Broker de-duplicates retries (producer ID + sequence numbers). Requires `WaitForAll` acks and `Net.MaxOpenRequests = 1` — retries can no longer interleave and reorder |

```go
config.Producer.Idempotent = true       // retries become safe
config.Producer.RequiredAcks = sarama.WaitForAll
config.Net.MaxOpenRequests = 1
config.Producer.Retry.Max = 5
config.Producer.Retry.Backoff = 100 * time.Millisecond
```

### 3.3 Throughput: batching & compression

| Config | Syntax | Meaning |
|---|---|---|
| `Producer.Flush.Frequency` | `100 * time.Millisecond` | Kafka's `linger.ms`: max wait before a partial batch ships |
| `Producer.Flush.Bytes` | `1 << 20` (1 MiB) | Kafka's `batch.size`: ship when the batch reaches this size |
| `Producer.Flush.MaxMessages` | `1000` | Batch trigger by message count |
| `Producer.Compression` | `sarama.CompressionLZ4` etc. | Compress each batch: `CompressionNone/GZIP/Snappy/LZ4/ZSTD` — fewer bytes on the wire ([Part 2 ch. 11](../part-2-kafka-deep-dive/11-performance.md)) |
| `Producer.MaxMessageBytes` | `1048576` | Max record size; keep payloads far below this — blobs belong in object storage |

```go
config.Producer.Flush.Frequency = 100 * time.Millisecond
config.Producer.Flush.MaxMessages = 500
config.Producer.Compression = sarama.CompressionLZ4
```

### 3.4 Choosing the key (the config that isn't a config)

`murmur2(key) % num_partitions` runs inside the client — the single most consequential producer decision:

```go
Key: sarama.StringEncoder(order.UserID) // ordering scope = per user
```

Key = the entity whose events must stay ordered (see [Part 2 ch. 08](../part-2-kafka-deep-dive/08-scalability.md)). No key = round-robin, even spread, no per-entity ordering.

## 4. A production-ready sync producer config, assembled

```go
config := sarama.NewConfig()
config.Version = sarama.V3_5_0_0

// durability
config.Producer.RequiredAcks = sarama.WaitForAll
config.Producer.Timeout = 5 * time.Second

// safe retries
config.Producer.Idempotent = true
config.Net.MaxOpenRequests = 1
config.Producer.Retry.Max = 5
config.Producer.Retry.Backoff = 100 * time.Millisecond

// throughput (tune to latency budget)
config.Producer.Compression = sarama.CompressionLZ4
config.Producer.Flush.Frequency = 50 * time.Millisecond

// sync producer requirement
config.Producer.Return.Successes = true
```

**Next:** [03 — consumer groups: the handler contract, offsets, rebalances](03-consumer.md)
