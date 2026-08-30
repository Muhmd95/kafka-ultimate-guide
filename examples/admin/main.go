package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

const (
	topic        = "orders"
	group        = "example-v1"
	partitionCnt = 3
)

func main() {
	brokers := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer admin.Close()

	if err := admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitionCnt,
		ReplicationFactor: 1,
	}, false); err != nil {
		log.Printf("create topic: %v (fine if it already exists)", err)
	}

	topics, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range topics {
		fmt.Printf("topic=%s partitions=%d\n", t.Name, len(t.Partitions))
	}

	var totalLag int64
	for p := int32(0); p < partitionCnt; p++ {
		logEnd, err := client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Fatal(err)
		}
		res, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {p}})
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
