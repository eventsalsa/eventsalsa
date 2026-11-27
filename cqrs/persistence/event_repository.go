package persistence

import (
	"context"
	"errors"
)

// EventRepository loads historical domain events for an aggregate stream and appends new
// events guarded by optimistic concurrency control against expectedVersion.
//
// Load returns an empty slice and version 0 (or ErrNotFound) when the stream does not exist yet.
// Append receives expectedVersion (0 when creating a new stream, or the loaded stream version)
// and must return ErrConcurrencyConflict when the persisted stream version differs.
type EventRepository[E any] interface {
	Load(ctx context.Context, id string) (events []E, version int64, err error)
	Append(ctx context.Context, id string, expectedVersion int64, events []E) error
}

// FromEventRepository binds an EventRepository[E] and Decider[S, E] into an Adapter[S, E]
// usable with NewCommandHandler.
//
// Historical events returned by Load are folded through decider.Initial and decider.Evolve,
// and non-empty emitted events are persisted via repo.Append(ctx, id, version, events).
func FromEventRepository[S any, E any](repo EventRepository[E], decider Decider[S, E]) Adapter[S, E] {
	return newAdapter(
		repo,
		decider,
		func(ctx context.Context, id string) (S, int64, error) {
			history, version, err := repo.Load(ctx, id)
			if errors.Is(err, ErrNotFound) {
				return decider.Initial(id), 0, nil
			}
			if err != nil {
				var zero S
				return zero, 0, err
			}
			return decider.fold(decider.Initial(id), history), version, nil
		},
		func(ctx context.Context, id string, expectedVersion int64, _ S, events []E) error {
			return repo.Append(ctx, id, expectedVersion, events)
		},
	)
}
