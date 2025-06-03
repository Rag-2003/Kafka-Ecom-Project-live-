package main

import (
	"log"
	"net/http"

	"kafka-producer/handlers"
)

func main() {
	http.HandleFunc("/order", handler.OrderHandler)

	log.Println("Starting server on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
	
}
