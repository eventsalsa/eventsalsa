package domain

import "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"

// Order holds pure write-side decision state reconstituted from persisted order facts.
type Order struct {
	ID         string
	CustomerID string
	TotalCents int64
	Placed     bool
	Confirmed  bool
}

// InitialOrder constructs the zero decision state for a given order identity.
func InitialOrder(id string) Order {
	return Order{ID: id}
}

// EvolveOrder applies a historical or newly emitted domain event to Order state.
func EvolveOrder(state Order, ev any) Order {
	switch e := ev.(type) {
	case events.OrderPlaced:
		state.CustomerID = e.CustomerID
		state.TotalCents = e.TotalCents
		state.Placed = true
	case events.OrderConfirmed:
		state.Confirmed = true
	}
	return state
}
