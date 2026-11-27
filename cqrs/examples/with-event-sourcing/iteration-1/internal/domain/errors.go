package domain

import "errors"

var (
	// ErrInvalidAmount indicates a non-positive order total.
	ErrInvalidAmount = errors.New("order amount must be positive")
	// ErrOrderNotFound indicates no order stream exists for the given identity.
	ErrOrderNotFound = errors.New("order not found")
	// ErrAlreadyPlaced indicates the order has already been placed.
	ErrAlreadyPlaced = errors.New("order already placed")
	// ErrNotPlaced indicates the order has not been placed yet.
	ErrNotPlaced = errors.New("order not placed")
)
