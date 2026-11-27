package domain

import "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"

// ConfirmOrder is an internal domain command decided by DecideConfirmOrder.
type ConfirmOrder struct {
	OrderID string
}

// DecideConfirmOrder validates confirmation invariants and emits OrderConfirmed when needed.
func DecideConfirmOrder(state Order, _ ConfirmOrder) ([]any, error) {
	if !state.Placed {
		return nil, ErrNotPlaced
	}
	if state.Confirmed {
		return nil, nil
	}
	return []any{events.OrderConfirmed{}}, nil
}
