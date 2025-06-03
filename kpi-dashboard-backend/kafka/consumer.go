package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"kpi-dashboard-backend/model"
)

// AddKPIFunc is a function type that accepts KPI events
type AddKPIFunc func(event model.KPIEvent)

// StartConsumer starts the Kafka consumer and sends KPI events via addKPI callback
func StartConsumer(addKPI AddKPIFunc) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "inventory-kpi",
		GroupID: "kpi-dashboard-group",
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("Kafka read error:", err)
			continue
		}

		var event model.KPIEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		addKPI(event) // Pass event to storage via callback
	}
}
