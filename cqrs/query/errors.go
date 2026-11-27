package query

import "errors"

var (
	// ErrNilQuery indicates a nil or typed-nil query was executed.
	ErrNilQuery = errors.New("query: query cannot be nil")

	// ErrInvalidResultType indicates a query middleware or caller expected a result type incompatible with R.
	ErrInvalidResultType = errors.New("query: result type mismatch")
)
