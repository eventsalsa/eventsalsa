package domain

import (
	"strings"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"
)

// AddOrderItem is an internal domain command decided by DecideAddOrderItem.
type AddOrderItem struct {
	OrderID   string
	SKU       string
	UnitCents int64
	Quantity  int64
}

// DecideAddOrderItem validates item addition invariants and emits OrderItemAdded.
func DecideAddOrderItem(state Order, cmd AddOrderItem) ([]any, error) {
	if !state.Placed {
		return nil, ErrNotPlaced
	}
	if state.Confirmed {
		return nil, ErrAlreadyConfirmed
	}
	if strings.TrimSpace(cmd.SKU) == "" {
		return nil, ErrEmptySKU
	}
	if cmd.UnitCents <= 0 || cmd.Quantity <= 0 {
		return nil, ErrInvalidAmount
	}
	return []any{events.OrderItemAdded{
		SKU:       cmd.SKU,
		UnitCents: cmd.UnitCents,
		Quantity:  cmd.Quantity,
	}}, nil
}
