package main

import "errors"

var (
	errInvalidAmount = errors.New("order amount must be positive")
	errAlreadyPlaced = errors.New("order already placed")
	errNotPlaced     = errors.New("order not placed")
)

// OrderPlaced records the creation of an order aggregate stream.
type OrderPlaced struct {
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
}

// OrderConfirmed records order confirmation.
type OrderConfirmed struct{}

// PlaceOrder requests placing a new order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// ConfirmOrder requests confirming an existing order.
type ConfirmOrder struct {
	OrderID string
}

// OrderState is the write-side decision state reconstituted from stream history.
type OrderState struct {
	ID         string
	CustomerID string
	TotalCents int64
	Placed     bool
	Confirmed  bool
}

func initialOrder(id string) OrderState {
	return OrderState{ID: id}
}

func evolveOrder(s OrderState, ev any) OrderState {
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

func decidePlaceOrder(s OrderState, cmd PlaceOrder) ([]any, error) {
	if s.Placed {
		return nil, errAlreadyPlaced
	}
	if cmd.TotalCents <= 0 {
		return nil, errInvalidAmount
	}
	return []any{OrderPlaced{CustomerID: cmd.CustomerID, TotalCents: cmd.TotalCents}}, nil
}

func decideConfirmOrder(s OrderState, _ ConfirmOrder) ([]any, error) {
	if !s.Placed {
		return nil, errNotPlaced
	}
	if s.Confirmed {
		return nil, nil
	}
	return []any{OrderConfirmed{}}, nil
}
