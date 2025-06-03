package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker = "localhost:9092"
	topic       = "OrderReceived"
)

func PublishOrderReceived(message string) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
	})

	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte("OrderKey"),
			Value: []byte(message),
		},
	)
	if err != nil {
		log.Println("Failed to write message:", err)
		return err
	}

	log.Println("Message published to Kafka topic:", topic)
	return nil
}
