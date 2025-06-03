package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type Notification struct {
	OrderID string `json:"orderId"`
	Message string `json:"message"`
}

func main() {
	fmt.Println("📢 Notification consumer started...")

	// Setup readers and writers
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
		GroupID: "notification-group",
	})
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})
	defer reader.Close()
	defer writer.Close()

	processed := make(map[string]bool)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("❌ Failed to read message:", err)
			continue
		}

		var notif Notification
		if err := json.Unmarshal(msg.Value, &notif); err != nil {
			log.Println("❌ Invalid notification event:", err)
			writer.WriteMessages(context.Background(), kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
			})
			continue
		}

		// Idempotency
		if processed[notif.OrderID] {
			continue
		}
		processed[notif.OrderID] = true

		log.Printf("🔔 Notification: OrderID=%s, Message=%s\n", notif.OrderID, notif.Message)
	}
}
