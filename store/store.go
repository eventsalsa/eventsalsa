package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

var (
	// ErrOptimisticConcurrency indicates a version conflict during append.
	ErrOptimisticConcurrency = errors.New("optimistic concurrency conflict")

	// ErrDuplicateEventID indicates an append attempted to reuse an existing event_id.
	ErrDuplicateEventID = errors.New("duplicate event id")

	// ErrNoEvents indicates an attempt to append zero events.
	ErrNoEvents = errors.New("no events to append")

	// ErrUnknownEvent indicates an unregistered or unsupported event type or schema version.
	ErrUnknownEvent = errors.New("unknown event")
)

// EventStore defines the interface for appending events.
type EventStore interface {
	// Append atomically appends one or more events within the provided transaction.
	// Events must all belong to the same stream instance.
	// Returns an AppendResult containing the persisted events with assigned versions
	// and their global positions, or an error.
	//
	// The expectedVersion parameter controls optimistic concurrency:
	// - Any(): No version check - always succeeds if no other errors
	// - NoStream(): Stream must not exist - used for stream creation
	// - Exact(N): Stream must be at version N - used for normal updates
	//
	// The store automatically assigns StreamVersion to each event:
	// - Fetches the current version from the stream_heads table (O(1) lookup)
	// - Validates against expectedVersion
	// - Assigns consecutive versions starting from (current + 1)
	// - Updates stream_heads with the new version
	// - The database unique constraint on (stream_type, stream_id, stream_version)
	//   enforces optimistic concurrency as a last safety net
	//
	// Returns ErrOptimisticConcurrency if expectedVersion validation fails or if
	// another transaction commits conflicting events between the version check and insert
	// (detected via unique constraint violation).
	// Returns ErrDuplicateEventID if an append attempts to reuse an existing event_id.
	// Returns ErrNoEvents if events slice is empty.
	//
	// After a successful append:
	// - Use result.ToVersion() to get the new stream version
	// - Use result.Events to access the persisted events with all fields populated (including TransactionID)
	// - Use result.GlobalPositions to get the assigned global positions
	Append(ctx context.Context, tx pgx.Tx, expectedVersion ExpectedVersion, events []Event) (AppendResult, error)
}

// Cursor identifies a persisted event's position in (TransactionID, GlobalPosition) order.
// The zero value starts before any persisted event.
type Cursor struct {
	TransactionID  uint64
	GlobalPosition int64
}

// Before reports whether c strictly precedes other in (TransactionID, GlobalPosition) order.
func (c Cursor) Before(other Cursor) bool {
	return c.TransactionID < other.TransactionID ||
		(c.TransactionID == other.TransactionID && c.GlobalPosition < other.GlobalPosition)
}

// IsZero reports whether the cursor is unset.
func (c Cursor) IsZero() bool {
	return c.TransactionID == 0 && c.GlobalPosition == 0
}

// EventReader defines the interface for reading events sequentially from the global log
// behind PostgreSQL's active transaction visibility horizon.
type EventReader interface {
	// ReadEvents returns up to limit committed events whose transaction_id precedes
	// pg_snapshot_xmin(pg_current_snapshot()) and whose (TransactionID, GlobalPosition)
	// tuple strictly follows after, ordered by (TransactionID ASC, GlobalPosition ASC).
	// Passing a zero store.Cursor reads from the beginning of the global log.
	//
	// WARNING: Direct SQL consumers must NOT tail the events table or persist checkpoints
	// using raw global_position alone. BIGSERIAL sequences guarantee uniqueness, not
	// commit order, so a lower global_position from an in-flight concurrent transaction can
	// commit after a higher one. Always tail using store.Cursor behind pg_snapshot_xmin
	// (via ReadEvents/GetHead or eventsalsa.dev/projector).
	ReadEvents(ctx context.Context, tx pgx.Tx, after Cursor, limit int) ([]PersistedEvent, error)
}

// HeadReader defines the interface for reading the latest event cursor currently
// settled behind PostgreSQL's active transaction visibility horizon.
// This is useful for lightweight "new settled events available" checks without loading batches.
type HeadReader interface {
	// GetHead returns the highest (TransactionID, GlobalPosition) cursor whose transaction_id
	// precedes pg_snapshot_xmin(pg_current_snapshot()), or the zero Cursor if none exists.
	//
	// WARNING: Direct SQL consumers must NOT use raw MAX(global_position) as a tailing cursor
	// because PostgreSQL sequence allocation order does not match commit visibility order.
	GetHead(ctx context.Context, tx pgx.Tx) (Cursor, error)
}

// StreamReader defines the interface for reading events for a specific stream.
type StreamReader interface {
	// ReadStream reads all events for a specific stream instance and returns
	// them as a Stream containing the stream's full history.
	// Events are ordered by stream_version ascending.
	//
	// Parameters:
	// - streamType: the type of stream (e.g., "User", "Order")
	// - streamID: the unique identifier of the stream instance (can be UUID string, email, etc.)
	// - fromVersion: optional minimum version (inclusive). Pass nil to read from the beginning.
	// - toVersion: optional maximum version (inclusive). Pass nil to read to the end.
	//
	// Examples:
	// - ReadStream(ctx, tx, "User", "550e8400-e29b-41d4-a716-446655440000", nil, nil) - read all events
	// - ReadStream(ctx, tx, "User", id, ptr(5), nil) - read from version 5 onwards
	// - ReadStream(ctx, tx, "User", id, nil, ptr(10)) - read up to version 10
	// - ReadStream(ctx, tx, "User", id, ptr(5), ptr(10)) - read versions 5-10
	//
	// Returns a Stream with an empty Events slice if no events match the criteria.
	// Use stream.Version() to get the current stream version.
	// Use stream.IsEmpty() to check if any events were found.
	ReadStream(ctx context.Context, tx pgx.Tx, streamType string, streamID string, fromVersion, toVersion *int64) (Stream, error)
}
