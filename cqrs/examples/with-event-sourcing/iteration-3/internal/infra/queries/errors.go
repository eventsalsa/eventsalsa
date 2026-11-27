package queries

import "errors"

// ErrEmptyQuery indicates a nil Protobuf query payload was passed to a read handler.
var ErrEmptyQuery = errors.New("query payload must not be nil")

// ErrMissingDB indicates GetOrderOverviewHandler was constructed without a database pool.
var ErrMissingDB = errors.New("queries: database pool cannot be nil")
