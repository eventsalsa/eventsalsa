package domain

// Status values represent authoritative order lifecycle states.
const (
	StatusPending  = "PENDING"
	StatusCanceled = "CANCELED"
)

// Order enforces write-side business invariants over order state.
type Order struct {
	ID         string
	CustomerID string
	Status     string
	TotalCents int64
}

// NewOrder validates creation invariants and constructs a pending Order.
func NewOrder(id, customerID string, totalCents int64) (Order, error) {
	if totalCents <= 0 {
		return Order{}, ErrInvalidOrderTotal
	}
	return Order{
		ID:         id,
		CustomerID: customerID,
		Status:     StatusPending,
		TotalCents: totalCents,
	}, nil
}

// Cancel transitions an active order to CANCELED, rejecting duplicate cancellations.
func (o *Order) Cancel() error {
	if o.Status == StatusCanceled {
		return ErrAlreadyCanceled
	}
	o.Status = StatusCanceled
	return nil
}
