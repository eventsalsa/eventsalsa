package domain

// OrderState is the write-side decision state reconstituted from stream history.
type OrderState struct {
	ID         string
	CustomerID string
	TotalCents int64
	Placed     bool
	Confirmed  bool
}

// InitialOrder constructs the zero decision state for an order stream.
func InitialOrder(id string) OrderState {
	return OrderState{ID: id}
}

// EvolveOrder applies a persisted domain fact onto OrderState without side effects.
func EvolveOrder(s OrderState, ev any) OrderState {
	switch e := ev.(type) {
	case OrderPlaced:
		s.CustomerID = e.CustomerID
		s.TotalCents = e.TotalCents
		s.Placed = true
	case OrderConfirmed:
		s.Confirmed = true
	}
	return s
}

// Place validates placement invariants against current state and emits OrderPlaced.
func Place(s OrderState, customerID string, totalCents int64) ([]any, error) {
	if s.Placed {
		return nil, ErrAlreadyPlaced
	}
	if totalCents <= 0 {
		return nil, ErrInvalidAmount
	}
	return []any{OrderPlaced{CustomerID: customerID, TotalCents: totalCents}}, nil
}

// Confirm validates confirmation invariants against current state and emits OrderConfirmed.
func Confirm(s OrderState) ([]any, error) {
	if !s.Placed {
		return nil, ErrNotPlaced
	}
	if s.Confirmed {
		return nil, nil
	}
	return []any{OrderConfirmed{}}, nil
}
