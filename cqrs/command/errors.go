package command

import "errors"

var (
	// ErrNilCommand indicates a nil or typed-nil command was dispatched.
	ErrNilCommand = errors.New("command: command cannot be nil")

	// ErrNilErrorSink indicates WithStrictOpaqueDispatch received a nil error callback.
	ErrNilErrorSink = errors.New("command: strict opaque error sink cannot be nil")
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
