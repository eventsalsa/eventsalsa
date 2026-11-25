package cqrs

import "errors"

var (
	// ErrNilBus indicates a nil command.Bus or query.Bus was supplied.
	ErrNilBus = errors.New("cqrs: bus cannot be nil")

	// ErrNilHandler indicates a nil or typed-nil handler was registered or wrapped.
	ErrNilHandler = errors.New("cqrs: handler cannot be nil")

	// ErrInvalidMessageType indicates a registered command or query type was an interface rather than concrete.
	ErrInvalidMessageType = errors.New("cqrs: message type must be concrete and non-interface")

	// ErrDuplicateHandler indicates a handler was already registered for the concrete message type.
	ErrDuplicateHandler = errors.New("cqrs: duplicate handler registered for message type")

	// ErrNoHandler indicates no handler is registered for the dispatched command or executed query type.
	ErrNoHandler = errors.New("cqrs: no handler registered for message type")
)
