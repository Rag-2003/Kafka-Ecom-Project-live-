package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type OrderPacked struct {
	OrderID string `json:"orderId"`
}

type Notification struct {
	OrderID string `json:"orderId"`
	Message string `json:"message"`
}

func main() {
	fmt.Println("🚚 Shipper consumer started...")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderPacked",
		GroupID: "shipper-group",
	})
	notifWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
	})
	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "DeadLetterQueue",
	})
	defer reader.Close()
	defer notifWriter.Close()
	defer dlqWriter.Close()

	processed := make(map[string]bool)

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("❌ Read error:", err)
			continue
		}

		var packed OrderPacked
		if err := json.Unmarshal(msg.Value, &packed); err != nil {
			log.Println("❌ Invalid OrderPacked event")
			dlqWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
			})
			continue
		}

		if processed[packed.OrderID] {
			continue
		}
		processed[packed.OrderID] = true

		log.Printf("📤 Order %s shipped", packed.OrderID)

		notif := Notification{
			OrderID: packed.OrderID,
			Message: fmt.Sprintf("Your order %s has been shipped!", packed.OrderID),
		}
		notifBytes, _ := json.Marshal(notif)
		notifWriter.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(packed.OrderID),
			Value: notifBytes,
		})
	}
}
