package domain

import (
	"slices"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain/events"
)

// Order is the write-side aggregate root maintaining invariants, loaded stream version,
// and uncommitted domain events.
type Order struct {
	ID          string
	CustomerID  string
	uncommitted []any
	TotalCents  int64
	version     int64
	Placed      bool
	Confirmed   bool
}

// PlaceOrder validates placement invariants and creates a new Order with an uncommitted OrderPlaced event.
func PlaceOrder(id, customerID string, totalCents int64) (*Order, error) {
	if totalCents <= 0 {
		return nil, ErrInvalidAmount
	}
	o := &Order{ID: id}
	o.record(events.OrderPlaced{
		CustomerID: customerID,
		TotalCents: totalCents,
	})
	return o, nil
}

// ReconstituteOrder creates an Order seeded with its identity and loaded stream version
// so repositories can reconstruct state by calling Evolve for each persisted event.
func ReconstituteOrder(id string, version int64) *Order {
	return &Order{
		ID:      id,
		version: version,
	}
}

// Evolve applies a historical domain event to Order state without recording an uncommitted change.
func (o *Order) Evolve(ev any) {
	switch e := ev.(type) {
	case events.OrderPlaced:
		o.CustomerID = e.CustomerID
		o.TotalCents = e.TotalCents
		o.Placed = true
	case events.OrderConfirmed:
		o.Confirmed = true
	}
}

// Confirm validates confirmation invariants and records OrderConfirmed when not yet confirmed.
func (o *Order) Confirm() error {
	if !o.Placed {
		return ErrNotPlaced
	}
	if o.Confirmed {
		return nil
	}
	o.record(events.OrderConfirmed{})
	return nil
}

// Version returns the loaded stream version used for optimistic concurrency checks on Save.
func (o *Order) Version() int64 {
	return o.version
}

// UncommittedEvents returns a copy of domain events recorded since creation or reconstitution.
func (o *Order) UncommittedEvents() []any {
	return slices.Clone(o.uncommitted)
}

func (o *Order) record(ev any) {
	o.Evolve(ev)
	o.uncommitted = append(o.uncommitted, ev)
}
