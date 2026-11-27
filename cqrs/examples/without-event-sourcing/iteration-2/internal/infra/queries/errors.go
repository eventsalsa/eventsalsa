package queries

import "errors"

// ErrEmptyQuery indicates a nil Protobuf query payload was passed to a read handler.
var ErrEmptyQuery = errors.New("query payload must not be nil")
