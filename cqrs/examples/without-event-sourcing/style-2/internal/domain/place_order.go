package domain

// PlaceOrder is the private domain command to create a customer order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}
