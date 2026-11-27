package domain

// PlaceOrder is a private domain command decided by DecidePlaceOrder.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// DecidePlaceOrder validates creation invariants against current state and emits OrderPlaced.
func DecidePlaceOrder(state Order, cmd PlaceOrder) ([]any, error) {
	if state.Status != "" {
		return nil, ErrAlreadyPlaced
	}
	if cmd.TotalCents <= 0 {
		return nil, ErrInvalidOrderTotal
	}
	return []any{OrderPlaced(cmd)}, nil
}
