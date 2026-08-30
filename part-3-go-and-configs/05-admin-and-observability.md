# 05 — Admin & Observability: Topics, Offsets, Lag

> Level: Intermediate

Running Kafka means creating things deliberately and answering one question endlessly: *are consumers keeping up?* This chapter is the Go toolkit for both.

---

## 1. ClusterAdmin: topics as code

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

	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	// CREATE — explicit beats auto-create (auto-create silently uses broker
	// defaults and enables "ghost topic" typos consuming nothing, loudly)
	err = admin.CreateTopic("orders", &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1, // 3 in production
	}, false)
	if err != nil {
		log.Fatal(err)
	}

	// DESCRIBE — partition count, leaders, ISR
	topics, _ := admin.DescribeTopics([]string{"orders"})
	for _, t := range topics {
		fmt.Printf("topic=%s partitions=%d\n", t.Name, len(t.Partitions))
	}

	// LIST everything
	all, _ := admin.ListTopics()
	for name, d := range all {
		fmt.Printf("%s partitions=%d\n", name, d.NumPartitions)
	}
}
```

Other useful calls: `DeleteTopic`, `CreatePartitions` (remember: new partitions re-map keys — ordering per key can break across old/new boundary), `ListConsumerGroups`, `DescribeConsumerGroups`.

## 2. Offsets and lag from Go

**Lag = log end offset − group's committed offset.** It is *the* health metric: lag climbing = consumer falling behind; lag draining to 0 = caught up.

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

	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatal(err)
	}

	const (
		topic = "orders"
		group = "billing-v1"
	)

	var totalLag int64
	for p := int32(0); p < 3; p++ {
		logEnd, err := client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}

		res, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{
			topic: {p},
		})
		if err != nil {
			log.Fatal(err)
		}
		committed := res.Blocks[topic][p].Offset

		lag := logEnd - committed
		totalLag += lag
		fmt.Printf("partition=%d committed=%d logEnd=%d lag=%d\n", p, committed, logEnd, lag)
	}
	fmt.Println("TOTAL LAG:", totalLag)
}
```

CLI equivalent (the quickest ad-hoc check):

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group billing-v1
# CURRENT-OFFSET, LOG-END-OFFSET, LAG per partition
```

## 3. What to watch in production

| Signal | Meaning | Typical alert |
|---|---|---|
| Consumer **lag** | Falling behind / stuck consumer | Lag > N or growing for M minutes |
| Lag = 0 but nothing processed | Consumer not running at all | Heartbeat/last-seen timestamp |
| **Under-replicated partitions** | Followers lagging — durability at risk | > 0 for any partition |
| Offline partitions | Broker/quorum trouble | > 0 |
| DLQ depth | Failing messages accumulating | Any growth |

Full operations treatment (JMX, failure scenarios, tuning): [Appendix B](../appendix/B-operations.md).

---

**Part 3 complete.** Continue with the [appendices](../appendix/) or the runnable project in [`examples/`](../examples/).
