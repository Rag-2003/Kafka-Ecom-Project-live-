package main

import (
    "encoding/json"
    "log"
    "net/http"

    "kpi-dashboard-backend/kafka"
    "kpi-dashboard-backend/storage"
    "kpi-dashboard-backend/model"
)

func main() {
    store := storage.NewInMemoryStore()

    // Pass a function with model.KPIEvent param
    go kafka.StartConsumer(func(event model.KPIEvent) {
        log.Println("Adding KPI event:", event)
        store.Add(event)
    })

    http.HandleFunc("/kpis", func(w http.ResponseWriter, r *http.Request) {
        log.Println("HTTP /kpis endpoint hit")
        kpis := store.GetAll()
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(kpis)
    })

    log.Println("Starting server on :8081")
    err := http.ListenAndServe(":8081", nil)
    if err != nil {
        log.Fatal("Server failed:", err)
    }
}
