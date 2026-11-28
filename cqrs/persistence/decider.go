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
// Construct an Adapter using FromStateRepository, FromEventRepository, or NewAdapter.
type Adapter[S any, E any] struct {
	exec func(ctx context.Context, id string, decideFn func(state S) ([]E, error)) error
	err  error
}

// NewAdapter wraps a custom persistence execution function into an Adapter[S, E] usable
// with NewCommandHandler. If fn is nil, NewCommandHandler returns ErrNilAdapter.
func NewAdapter[S any, E any](
	fn func(ctx context.Context, id string, decide func(state S) ([]E, error)) error,
) Adapter[S, E] {
	if fn == nil {
		return Adapter[S, E]{err: ErrNilAdapter}
	}
	return Adapter[S, E]{exec: fn}
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
