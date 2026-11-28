package domain

import "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"

// Order holds pure write-side decision state reconstituted from snapshots and/or domain events.
// Version counts applied domain events so snapshot infrastructure can verify state parity.
type Order struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
	ItemCount  int64  `json:"item_count"`
	Version    int64  `json:"version"`
	Placed     bool   `json:"placed"`
	Confirmed  bool   `json:"confirmed"`
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
		state.Version++
	case events.OrderItemAdded:
		state.ItemCount += e.Quantity
		state.TotalCents += e.UnitCents * e.Quantity
		state.Version++
	case events.OrderConfirmed:
		state.Confirmed = true
		state.Version++
	}
	return state
}
