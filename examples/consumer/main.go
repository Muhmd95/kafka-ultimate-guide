package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type handler struct{}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("topic=%s partition=%d offset=%d key=%s value=%s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Interval = time.Second

	group, err := sarama.NewConsumerGroup(brokers, "example-v1", config)
	if err != nil {
		log.Fatal(err)
	}
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	h := &handler{}
	for {
		if err := group.Consume(ctx, []string{"orders"}, h); err != nil {
			log.Printf("session ended with error: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}
