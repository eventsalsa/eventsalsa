package commands

// PlaceOrder requests placing a new customer order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}
