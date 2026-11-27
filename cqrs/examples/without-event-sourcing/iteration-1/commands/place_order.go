package commands

// PlaceOrder requests creating a new order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}
