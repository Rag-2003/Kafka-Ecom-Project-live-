package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type OrderConfirmed struct {
	OrderID string `json:"orderId"`
}

type OrderPacked struct {
	OrderID string `json:"orderId"`
}

func main() {
	fmt.Println("🏬 Warehouse consumer started...")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderConfirmed",
		GroupID: "warehouse-group",
	})
	packedWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderPacked",
	})
	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})
	defer reader.Close()
	defer packedWriter.Close()
	defer dlqWriter.Close()

	processed := make(map[string]bool)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("❌ Read error:", err)
			continue
		}

		var event OrderConfirmed
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("❌ Invalid OrderConfirmed event")
			dlqWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
			})
			continue
		}

		if processed[event.OrderID] {
			continue
		}
		processed[event.OrderID] = true

		log.Printf("📦 Warehouse packing order: %s\n", event.OrderID)

		packedBytes, _ := json.Marshal(OrderPacked{OrderID: event.OrderID})
		packedWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.OrderID),
			Value: packedBytes,
		})
	}
}
