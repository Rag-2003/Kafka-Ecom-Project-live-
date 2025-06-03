package kafka

import (
	"context"
	"encoding/json"
	"inventory-consumer/kpi"
	"inventory-consumer/model"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

var processedOrders = make(map[string]bool)

func publishErrorKPI(writer *kafka.Writer, errorType string) {
	event := kpi.KPIEvent{
		KPIName:     "InventoryProcessingErrors",
		MetricName:  errorType,
		MetricValue: 1,
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	if err := kpi.PublishKPI(writer, event); err != nil {
		log.Printf("❌ Failed to publish error KPI: %v", err)
	}
}

func publishLatencyKPI(writer *kafka.Writer, latency time.Duration) {
	event := kpi.KPIEvent{
		KPIName:     "OrderProcessingLatency",
		MetricName:  "inventory-processing-latency-ms",
		MetricValue: float64(latency.Milliseconds()),
		Timestamp:   time.Now().Format(time.RFC3339),
	}
	if err := kpi.PublishKPI(writer, event); err != nil {
		log.Printf("❌ Failed to publish latency KPI: %v", err)
	}
}

func StartConsumer() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "OrderReceived",
		GroupID:   "inventory-consumer-group",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()

	kpiWriter := NewKPIWriter()
	defer kpiWriter.Close()

	log.Println("📦 Inventory Consumer started...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("❌ Error reading message: %v", err)
			continue
		}

		start := time.Now()

		var order model.Order
		if err := json.Unmarshal(m.Value, &order); err != nil {
			log.Printf("❌ Invalid JSON: %v", err)
			PublishToTopic("DeadLetterQueue", string(m.Value))
			publishErrorKPI(kpiWriter, "Invalid JSON")
			continue
		}

		if order.OrderID == "" || order.Item == "" || order.Amount == 0 {
			log.Printf("❌ Missing required fields: %+v", order)
			PublishToTopic("DeadLetterQueue", string(m.Value))
			publishErrorKPI(kpiWriter, "Missing fields")
			continue
		}

		if processedOrders[order.OrderID] {
			log.Printf("⚠️ Duplicate OrderID: %s", order.OrderID)
			publishErrorKPI(kpiWriter, "Duplicate OrderID")
			continue
		}

		processedOrders[order.OrderID] = true
		log.Printf("✅ Valid Order: %+v", order)

		orderBytes, _ := json.Marshal(order)
		if err := PublishToTopic("OrderConfirmed", string(orderBytes)); err != nil {
			log.Printf("❌ Failed to publish to OrderConfirmed")
			publishErrorKPI(kpiWriter, "Failed to publish OrderConfirmed")
			continue
		}

		latency := time.Since(start)
		publishLatencyKPI(kpiWriter, latency)
	}
}
