package main

import "context"

// GetOrderSummary fetches a display DTO for screens or API responses.
type GetOrderSummary struct {
	OrderID string
}

// OrderSummary is a presentation read model shaped specifically for callers.
type OrderSummary struct {
	OrderID      string
	CustomerName string
	Status       string
	DisplayTotal string
}

// GetOrderSummaryHandler implements query.Handler[GetOrderSummary, OrderSummary].
type GetOrderSummaryHandler struct {
	db *appDB
}

// Handle reads the presentation summary directly without domain validation.
func (h *GetOrderSummaryHandler) Handle(_ context.Context, q GetOrderSummary) (OrderSummary, error) {
	return h.db.selectOrderSummary(q.OrderID)
}
