package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

var brokerList = []string{"localhost:9093"}

const (
	topic    = "important"
	maxRetry = 1
)

func main() {
	log.Println("Starting a new Sarama sync producer")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panic(err)
		}
	}()
	for i := 1; i < 2000; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("Sample message #%d", i)),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	}
}

// func main() {
// 	Publish("localhost:9092", "important", "Hello, Kafka!")
// }

// func Publish(brokerList string, topic string, message string) {
// 	producerConfig := sarama.NewConfig()
// 	producerConfig.Producer.Return.Successes = true
// 	producer, err := sarama.NewSyncProducer([]string{brokerList}, producerConfig)
// 	if err != nil {
// 		log.Fatalln("Failed to start Sarama producer:", err)
// 	}

// 	msg := &sarama.ProducerMessage{
// 		Topic: topic,
// 		Value: sarama.StringEncoder(message),
// 	}

// 	_, _, err = producer.SendMessage(msg)
// 	if err != nil {
// 		log.Fatalln("Failed to publish message to topic", topic, err)
// 	}

// 	log.Println("Message published to topic", topic)
// }
