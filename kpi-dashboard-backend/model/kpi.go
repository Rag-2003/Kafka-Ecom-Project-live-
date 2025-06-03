package model

type KPIEvent struct {
	KPIName     string  `json:"kpi_name"`
	MetricName  string  `json:"metric_name"`
	MetricValue float64 `json:"metric_value"`
	Timestamp   string  `json:"timestamp"`
}
