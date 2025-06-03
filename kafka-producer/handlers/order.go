package handler

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"kafka-producer/kafka"
)

type Order struct {
	OrderID string `json:"orderId"`
	Item    string `json:"item"`
	Amount  int    `json:"amount"`
}

func OrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	var order Order
	if err := json.Unmarshal(body, &order); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("Received Order: %+v\n", order)

	if err := kafka.PublishOrderReceived(string(body)); err != nil {
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Order received"))
}
