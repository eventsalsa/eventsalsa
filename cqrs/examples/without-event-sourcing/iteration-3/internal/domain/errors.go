package domain

import "errors"

var (
	// ErrInvalidOrderTotal indicates a non-positive order total.
	ErrInvalidOrderTotal = errors.New("order total must be positive")
	// ErrAlreadyPlaced indicates the order has already been created.
	ErrAlreadyPlaced = errors.New("order already placed")
	// ErrNotPlaced indicates a command targeted an order that has not been placed yet.
	ErrNotPlaced = errors.New("order not placed")
	// ErrAlreadyCanceled indicates the order has already been canceled.
	ErrAlreadyCanceled = errors.New("order already canceled")
	// ErrCancellationReasonRequired indicates a blank cancellation reason was provided.
	ErrCancellationReasonRequired = errors.New("cancellation reason must not be blank")
)
