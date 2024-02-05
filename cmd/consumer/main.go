package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

var (
	brokerList = []string{"localhost:9093"}
	topic      = "important"
)

type ExampleConsumerGroupHandler struct{}

func (ExampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	// Implement setup here
	return nil
}

func (ExampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// Implement cleanup here
	return nil
}

func (ExampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// log.Printf("Message topic:%q partition:%d offset:%d\n- Processando: %s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		log.Printf("- Processing: %s\n", msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // Set appropriate Kafka version
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	brokers := brokerList

	group, err := sarama.NewConsumerGroup(brokers, "important", config)
	if err != nil {
		log.Panic(err)
	}

	consumer := ExampleConsumerGroupHandler{}

	ctx := context.Background()
	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := group.Consume(ctx, []string{topic}, consumer); err != nil {
			log.Panic(err)
		}
	}
}
