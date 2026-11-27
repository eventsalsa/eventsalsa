package domain

import "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain/events"

// Order is the write-side aggregate root reconstituted from stream history.
type Order struct {
	ID         string
	CustomerID string
	TotalCents int64
	Placed     bool
	Confirmed  bool
}

// InitialOrder constructs the zero Order aggregate for an order stream.
func InitialOrder(id string) Order {
	return Order{ID: id}
}

// EvolveOrder applies a persisted domain fact onto Order without side effects.
func EvolveOrder(o Order, ev any) Order {
	switch e := ev.(type) {
	case events.OrderPlaced:
		o.CustomerID = e.CustomerID
		o.TotalCents = e.TotalCents
		o.Placed = true
	case events.OrderConfirmed:
		o.Confirmed = true
	}
	return o
}

// Place validates placement invariants against current Order state and emits OrderPlaced.
func Place(o Order, customerID string, totalCents int64) ([]any, error) {
	if o.Placed {
		return nil, ErrAlreadyPlaced
	}
	if totalCents <= 0 {
		return nil, ErrInvalidAmount
	}
	return []any{events.OrderPlaced{CustomerID: customerID, TotalCents: totalCents}}, nil
}

// Confirm validates confirmation invariants against current Order state and emits OrderConfirmed.
func Confirm(o Order) ([]any, error) {
	if !o.Placed {
		return nil, ErrNotPlaced
	}
	if o.Confirmed {
		return nil, nil
	}
	return []any{events.OrderConfirmed{}}, nil
}
