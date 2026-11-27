package domain

// PlaceOrder is the private domain command to create a customer order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// HandlePlaceOrder is a pure, side-effect-free domain handler that validates
// creation invariants and returns a pending Order.
func HandlePlaceOrder(cmd PlaceOrder) (Order, error) {
	if cmd.TotalCents <= 0 {
		return Order{}, ErrInvalidOrderTotal
	}
	return Order{
		ID:         cmd.OrderID,
		CustomerID: cmd.CustomerID,
		Status:     StatusPending,
		TotalCents: cmd.TotalCents,
	}, nil
}
