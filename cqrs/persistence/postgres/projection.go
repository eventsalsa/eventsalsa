package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
)

// Projection processes a single persisted event inside the write transaction.
// Matches eventsalsa.dev/projector.Projection without requiring a projector dependency.
type Projection interface {
	Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error
}

// ProjectionFunc adapts a function into an inline Projection.
type ProjectionFunc func(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error

// Handle invokes f(ctx, tx, event).
//
//nolint:gocritic // hugeParam: matches store.PersistedEvent value contract
func (f ProjectionFunc) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	return f(ctx, tx, event)
}

// BatchProjection processes a batch of persisted events inside the write transaction.
// Matches eventsalsa.dev/projector.BatchProjection without requiring a projector dependency.
type BatchProjection interface {
	Handle(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error
}

// BatchProjectionFunc adapts a function into an inline BatchProjection.
type BatchProjectionFunc func(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error

// Handle invokes f(ctx, tx, events).
func (f BatchProjectionFunc) Handle(ctx context.Context, tx pgx.Tx, events []store.PersistedEvent) error {
	return f(ctx, tx, events)
}

func validateProjections(projections []Projection, batches []BatchProjection) error {
	for _, p := range projections {
		if isNilValue(p) {
			return ErrNilProjection
		}
	}
	for _, b := range batches {
		if isNilValue(b) {
			return ErrNilProjection
		}
	}
	return nil
}
