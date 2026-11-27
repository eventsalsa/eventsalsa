package persistence

import (
	"context"
	"errors"
)

// StateRepository persists folded aggregate state S guarded by optimistic concurrency control.
//
// Load must return ErrNotFound when no record exists yet for id.
// Save receives expectedVersion (0 when creating a new record, or the version returned by Load
// when updating an existing record) and must return ErrConcurrencyConflict if the persisted
// version does not match expectedVersion.
type StateRepository[S any] interface {
	Load(ctx context.Context, id string) (state S, version int64, err error)
	Save(ctx context.Context, id string, expectedVersion int64, state S) error
}

// FromStateRepository binds a StateRepository[S] and Decider[S, E] into an Adapter[S, E]
// usable with NewCommandHandler.
//
// Missing records (Load returning ErrNotFound) start from decider.Initial(id) at version 0;
// non-empty emitted events are folded through decider.Evolve and saved with expectedVersion.
func FromStateRepository[S any, E any](repo StateRepository[S], decider Decider[S, E]) Adapter[S, E] {
	return newAdapter(
		repo,
		decider,
		func(ctx context.Context, id string) (S, int64, error) {
			state, version, err := repo.Load(ctx, id)
			if errors.Is(err, ErrNotFound) {
				return decider.Initial(id), 0, nil
			}
			return state, version, err
		},
		func(ctx context.Context, id string, expectedVersion int64, prevState S, events []E) error {
			nextState := decider.fold(prevState, events)
			return repo.Save(ctx, id, expectedVersion, nextState)
		},
	)
}
