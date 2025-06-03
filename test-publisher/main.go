// warehouse/main.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

type OrderReceived struct {
	OrderID string `json:"orderId"`
	Item    string `json:"item"`
}

type Notification struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "OrderReceived",
		GroupID: "warehouse-group",
	})
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "Notification",
	})

	defer reader.Close()
	defer writer.Close()

	fmt.Println("🏭 Warehouse is listening for OrderReceived events...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("Error reading message: ", err)
		}

		var event OrderReceived
		json.Unmarshal(m.Value, &event)

		fmt.Printf("🏭 Received Order: %+v\n", event)

		notification := Notification{
			OrderID: event.OrderID,
			Status:  "OrderPacked",
		}
		data, _ := json.Marshal(notification)

		writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.OrderID),
			Value: data,
		})

		fmt.Printf("📦 Notification sent: %+v\n", notification)
	}
}
