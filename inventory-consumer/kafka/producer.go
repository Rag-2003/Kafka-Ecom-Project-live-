package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

func PublishToTopic(topic string, value string) error {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	err := w.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(value),
	})
	if err != nil {
		log.Printf("❌ Failed to publish to topic %s: %v", topic, err)
	}
	return err
}

func NewKPIWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "inventory-kpi",
		Balancer: &kafka.LeastBytes{},
	}
}
