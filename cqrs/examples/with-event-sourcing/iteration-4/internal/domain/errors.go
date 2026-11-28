package domain

import "errors"

var (
	// ErrInvalidAmount indicates a non-positive order amount, unit price, or item quantity.
	ErrInvalidAmount = errors.New("order amount and quantity must be positive")
	// ErrAlreadyPlaced indicates the order has already been placed.
	ErrAlreadyPlaced = errors.New("order already placed")
	// ErrAlreadyConfirmed indicates the order has already been confirmed.
	ErrAlreadyConfirmed = errors.New("order already confirmed")
	// ErrNotPlaced indicates the order has not been placed yet.
	ErrNotPlaced = errors.New("order not placed")
	// ErrEmptySKU indicates a line item SKU was empty or whitespace.
	ErrEmptySKU = errors.New("sku must not be blank")
)

// IsRejection reports whether err represents a domain invariant violation rather than an
// infrastructure failure.
func IsRejection(err error) bool {
	return errors.Is(err, ErrInvalidAmount) ||
		errors.Is(err, ErrAlreadyPlaced) ||
		errors.Is(err, ErrAlreadyConfirmed) ||
		errors.Is(err, ErrNotPlaced) ||
		errors.Is(err, ErrEmptySKU)
}
