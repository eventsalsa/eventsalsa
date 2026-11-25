package cqrs

import "errors"

var (
	// ErrNilBus indicates a nil CommandBus or QueryBus was supplied.
	ErrNilBus = errors.New("cqrs: bus cannot be nil")

	// ErrNilCommand indicates a nil or typed-nil command was dispatched.
	ErrNilCommand = errors.New("cqrs: command cannot be nil")

	// ErrNilQuery indicates a nil or typed-nil query was executed.
	ErrNilQuery = errors.New("cqrs: query cannot be nil")

	// ErrNilHandler indicates a nil or typed-nil handler was registered or wrapped.
	ErrNilHandler = errors.New("cqrs: handler cannot be nil")

	// ErrNilErrorSink indicates WithStrictOpaqueDispatch received a nil error callback.
	ErrNilErrorSink = errors.New("cqrs: strict opaque error sink cannot be nil")

	// ErrInvalidMessageType indicates a registered command or query type was an interface rather than concrete.
	ErrInvalidMessageType = errors.New("cqrs: message type must be concrete and non-interface")

	// ErrDuplicateHandler indicates a handler was already registered for the concrete message type.
	ErrDuplicateHandler = errors.New("cqrs: duplicate handler registered for message type")

	// ErrNoHandler indicates no handler is registered for the dispatched command or query type.
	ErrNoHandler = errors.New("cqrs: no handler registered for message type")

	// ErrInvalidResultType indicates a query middleware returned a value incompatible with R.
	ErrInvalidResultType = errors.New("cqrs: query result type mismatch")
)

type rejectedError struct {
	err error
}

func (e *rejectedError) Error() string {
	return e.err.Error()
}

func (e *rejectedError) Unwrap() error {
	return e.err
}

// Reject marks err as a deterministic domain or business rejection rather than an
// unhandled infrastructure failure. Returns nil if err is nil, and preserves err's chain.
func Reject(err error) error {
	if err == nil {
		return nil
	}
	if IsRejected(err) {
		return err
	}
	return &rejectedError{err: err}
}

// IsRejected reports whether err (or any error in its tree) is marked with Reject.
func IsRejected(err error) bool {
	if err == nil {
		return false
	}
	var target *rejectedError
	return errors.As(err, &target)
}
