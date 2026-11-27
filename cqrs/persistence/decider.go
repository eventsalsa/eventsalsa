package persistence

import "context"

// Decider defines pure state initialization and evolution functions for state type S
// over domain event type E, matching the function signatures used by cqrs/spectest.
type Decider[S any, E any] struct {
	Initial func(id string) S
	Evolve  func(state S, event E) S
}

func (d Decider[S, E]) validate() error {
	if d.Initial == nil {
		return ErrNilInitial
	}
	if d.Evolve == nil {
		return ErrNilEvolve
	}
	return nil
}

func (d Decider[S, E]) fold(state S, events []E) S {
	for i := range events {
		state = d.Evolve(state, events[i])
	}
	return state
}

// Adapter binds a StateRepository or EventRepository to a Decider[S, E] so
// NewCommandHandler can execute load, fold, decide, and optimistic-concurrency commit
// identically across state-based and event-sourced storage.
//
// Construct an Adapter using FromStateRepository or FromEventRepository.
type Adapter[S any, E any] struct {
	exec func(ctx context.Context, id string, decideFn func(state S) ([]E, error)) error
	err  error
}

func newAdapter[S any, E any](
	repo any,
	decider Decider[S, E],
	loadState func(ctx context.Context, id string) (S, int64, error),
	persist func(ctx context.Context, id string, expectedVersion int64, prevState S, events []E) error,
) Adapter[S, E] {
	if isNilValue(repo) {
		return Adapter[S, E]{err: ErrNilRepository}
	}
	if err := decider.validate(); err != nil {
		return Adapter[S, E]{err: err}
	}
	return Adapter[S, E]{
		exec: func(ctx context.Context, id string, decideFn func(S) ([]E, error)) error {
			state, version, loadErr := loadState(ctx, id)
			if loadErr != nil {
				return loadErr
			}
			if version < 0 {
				return ErrInvalidVersion
			}

			events, decideErr := decideFn(state)
			if decideErr != nil {
				return decideErr
			}
			if len(events) == 0 {
				return nil
			}
			return persist(ctx, id, version, state, events)
		},
	}
}
