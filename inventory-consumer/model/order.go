package model

type Order struct {
    OrderID string  `json:"order_id"`
    Item    string  `json:"item"`
    Amount  float64 `json:"amount"`
}


