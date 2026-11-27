package domain

import "errors"

var (
	// ErrInvalidOrderTotal indicates a non-positive order total.
	ErrInvalidOrderTotal = errors.New("order total must be positive")
	// ErrOrderNotFound indicates the target order does not exist.
	ErrOrderNotFound = errors.New("order not found")
	// ErrAlreadyCanceled indicates the order has already been canceled.
	ErrAlreadyCanceled = errors.New("order already canceled")
)
