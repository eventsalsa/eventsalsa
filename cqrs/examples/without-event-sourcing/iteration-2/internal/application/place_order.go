package application

// PlaceOrder is the internal use-case input for placing a customer order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}
