package postgres

import (
	"errors"

	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/store"
)

var (
	// ErrNilDB indicates a nil or typed-nil TxBeginner was supplied.
	ErrNilDB = errors.New("postgres: db cannot be nil")

	// ErrNilTx indicates a nil or typed-nil pgx.Tx was supplied.
	ErrNilTx = errors.New("postgres: tx cannot be nil")

	// ErrTxAlreadyActive indicates a transaction is already active in context.
	ErrTxAlreadyActive = errors.New("postgres: transaction already active in context")

	// ErrMissingTx indicates no active pgx.Tx was found in context for RequireTx or TxEventRepository.
	ErrMissingTx = errors.New("postgres: missing active transaction in context")

	// ErrNilFunc indicates a nil callback function was supplied.
	ErrNilFunc = errors.New("postgres: callback func cannot be nil")

	// ErrNilEventStore indicates a nil or typed-nil store.EventStore was supplied.
	ErrNilEventStore = errors.New("postgres: event store cannot be nil")

	// ErrNilStreamReader indicates a nil or typed-nil store.StreamReader was supplied or available.
	ErrNilStreamReader = errors.New("postgres: stream reader cannot be nil")

	// ErrNilCodec indicates a nil or incomplete EventCodec was supplied.
	ErrNilCodec = errors.New("postgres: event codec cannot be nil")

	// ErrEmptyStreamType indicates a blank StreamType was supplied.
	ErrEmptyStreamType = errors.New("postgres: stream type must not be blank")

	// ErrEmptyEventType indicates EventCodec.Encode returned a blank EventType.
	ErrEmptyEventType = errors.New("postgres: encoded event type must not be blank")

	// ErrNilProjection indicates a nil or typed-nil inline projection was supplied.
	ErrNilProjection = errors.New("postgres: inline projection cannot be nil")

	// ErrStreamMismatch indicates EventCodec.Encode returned an unexpected StreamType or StreamID.
	ErrStreamMismatch = errors.New("postgres: encoded event stream mismatch")

	// ErrNilSnapshotStore indicates a nil or typed-nil snapshot.Store was supplied.
	ErrNilSnapshotStore = errors.New("postgres: snapshot store cannot be nil")

	// ErrNilSnapshotCodec indicates a nil or typed-nil snapshot.Codec was supplied.
	ErrNilSnapshotCodec = errors.New("postgres: snapshot codec cannot be nil")

	// ErrNilSnapshotPolicy indicates a nil snapshot.Policy was supplied.
	ErrNilSnapshotPolicy = errors.New("postgres: snapshot policy cannot be nil")

	// ErrNilPayloadTransformer indicates a nil or typed-nil snapshot.PayloadTransformer was supplied.
	ErrNilPayloadTransformer = errors.New("postgres: snapshot payload transformer cannot be nil")
)

type concurrencyConflictError struct {
	err error
}

func (e *concurrencyConflictError) Error() string {
	return e.err.Error()
}

func (e *concurrencyConflictError) Unwrap() []error {
	return []error{persistence.ErrConcurrencyConflict, e.err}
}

func wrapConcurrencyConflict(err error) error {
	if errors.Is(err, persistence.ErrConcurrencyConflict) {
		return err
	}
	return &concurrencyConflictError{err: err}
}

// IsConcurrencyConflict reports whether err represents an optimistic concurrency conflict
// from either eventsalsa.dev/cqrs/persistence or eventsalsa.dev/store.
func IsConcurrencyConflict(err error) bool {
	return errors.Is(err, persistence.ErrConcurrencyConflict) ||
		errors.Is(err, store.ErrOptimisticConcurrency)
}
