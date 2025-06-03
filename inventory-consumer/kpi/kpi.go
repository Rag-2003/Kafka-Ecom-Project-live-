package kpi

import (
	"context"
	"encoding/json"
	

	"github.com/segmentio/kafka-go"
)

type KPIEvent struct {
	KPIName     string  `json:"kpi_name"`
	MetricName  string  `json:"metric_name"`
	MetricValue float64 `json:"metric_value"`
	Timestamp   string  `json:"timestamp"`
}

func PublishKPI(writer *kafka.Writer, event KPIEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	})
}
