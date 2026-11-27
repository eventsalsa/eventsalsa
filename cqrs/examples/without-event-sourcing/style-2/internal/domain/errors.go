package domain

import "errors"

var (
	// ErrInvalidOrderTotal indicates a non-positive order total.
	ErrInvalidOrderTotal = errors.New("order total must be positive")
	// ErrOrderNotFound indicates the target order does not exist.
	ErrOrderNotFound = errors.New("order not found")
	// ErrAlreadyCanceled indicates the order has already been canceled.
	ErrAlreadyCanceled = errors.New("order already canceled")
	// ErrEmptyCommand indicates a nil transport command was passed across the ACL boundary.
	ErrEmptyCommand = errors.New("command payload must not be nil")
	// ErrEmptyQuery indicates a nil transport query was passed to a query handler.
	ErrEmptyQuery = errors.New("query payload must not be nil")
)
