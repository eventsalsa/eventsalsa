package domain

import "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"

// PlaceOrder is an internal domain command decided by DecidePlaceOrder.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// DecidePlaceOrder validates placement invariants and emits OrderPlaced.
func DecidePlaceOrder(state Order, cmd PlaceOrder) ([]any, error) {
	if state.Placed {
		return nil, ErrAlreadyPlaced
	}
	if cmd.TotalCents <= 0 {
		return nil, ErrInvalidAmount
	}
	return []any{events.OrderPlaced{
		CustomerID: cmd.CustomerID,
		TotalCents: cmd.TotalCents,
	}}, nil
}
