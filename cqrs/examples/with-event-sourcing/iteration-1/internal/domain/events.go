package domain

// OrderPlaced records the creation of an order aggregate stream.
type OrderPlaced struct {
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
}

// OrderConfirmed records order confirmation.
type OrderConfirmed struct{}
