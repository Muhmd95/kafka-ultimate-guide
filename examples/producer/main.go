package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}

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

	// throughput
	config.Producer.Compression = sarama.CompressionLZ4
	config.Producer.Flush.Frequency = 50 * time.Millisecond

	// sync producer requirement
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	fmt.Println("type messages as 'key value' (empty line to quit):")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			break
		}
		key, value, _ := strings.Cut(line, " ")

		msg := &sarama.ProducerMessage{
			Topic: "orders",
			Key:   sarama.StringEncoder(key),
			Value: sarama.ByteEncoder(value),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("publish failed: %v", err)
			continue
		}
		fmt.Printf("ok partition=%d offset=%d\n", partition, offset)
	}
}
