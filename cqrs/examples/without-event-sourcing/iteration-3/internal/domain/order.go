package domain

// Status values represent authoritative order lifecycle states.
const (
	StatusPending  = "PENDING"
	StatusCanceled = "CANCELED"
)

// Order holds the write-side decision state reconstituted from state persistence or history.
type Order struct {
	ID         string
	CustomerID string
	Status     string
	TotalCents int64
}

// InitialOrder constructs the zero decision state for a given order identity.
func InitialOrder(id string) Order {
	return Order{ID: id}
}

// EvolveOrder applies a domain fact to Order state without side effects.
func EvolveOrder(state Order, ev any) Order {
	switch e := ev.(type) {
	case OrderPlaced:
		state.ID = e.OrderID
		state.CustomerID = e.CustomerID
		state.Status = StatusPending
		state.TotalCents = e.TotalCents
	case OrderCanceled:
		state.Status = StatusCanceled
	}
	return state
}
