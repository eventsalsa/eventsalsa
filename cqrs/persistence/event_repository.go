package persistence

import (
	"context"
	"errors"
)

// EventRepository loads historical domain events for an aggregate stream and appends new
// events guarded by optimistic concurrency control against expectedVersion.
// Load returns an empty slice and version 0 (or ErrNotFound) when the stream does not exist yet.
type EventRepository[E any] interface {
	Load(ctx context.Context, id string) (events []E, version int64, err error)
	Append(ctx context.Context, id string, expectedVersion int64, events []E) error
}

// EventRunner binds an EventRepository to a Decider so command decision functions reconstitute
// state from stream history and optimistically append emitted events.
type EventRunner[S any, E any] struct {
	repo    EventRepository[E]
	decider Decider[S, E]
}

// NewEventRunner constructs a validated EventRunner backed by repo and decider.
func NewEventRunner[S any, E any](repo EventRepository[E], decider Decider[S, E]) (*EventRunner[S, E], error) {
	if err := validateDependencies(repo, decider); err != nil {
		return nil, err
	}
	return &EventRunner[S, E]{
		repo:    repo,
		decider: decider,
	}, nil
}

// MustNewEventRunner calls NewEventRunner and panics if any dependency is invalid.
func MustNewEventRunner[S any, E any](repo EventRepository[E], decider Decider[S, E]) *EventRunner[S, E] {
	runner, err := NewEventRunner(repo, decider)
	if err != nil {
		panic(err)
	}
	return runner
}

// Load reads stream history for id and folds it through Initial and Evolve, returning the
// reconstituted state and loaded stream version. Missing streams return Initial(id) at version 0.
func (r *EventRunner[S, E]) Load(ctx context.Context, id string) (state S, version int64, err error) {
	if r == nil {
		return state, 0, ErrNilRunner
	}
	if valErr := validateTarget(ctx, id); valErr != nil {
		return state, 0, valErr
	}

	history, loadedVersion, loadErr := r.repo.Load(ctx, id)
	if errors.Is(loadErr, ErrNotFound) {
		return r.decider.Initial(id), 0, nil
	}
	if loadErr != nil {
		return state, 0, loadErr
	}
	if loadedVersion < 0 {
		return state, 0, ErrInvalidVersion
	}

	state = r.decider.Initial(id)
	for i := range history {
		state = r.decider.Evolve(state, history[i])
	}
	return state, loadedVersion, nil
}

// Run reconstitutes state for id, invokes decide, and appends any emitted events against the
// loaded expectedVersion. Zero-event decisions exit cleanly without calling Append.
func (r *EventRunner[S, E]) Run(ctx context.Context, id string, decide func(state S) ([]E, error)) error {
	if r == nil {
		return ErrNilRunner
	}
	if err := validateRun(ctx, id, decide); err != nil {
		return err
	}

	state, version, err := r.Load(ctx, id)
	if err != nil {
		return err
	}

	events, decideErr := decide(state)
	if decideErr != nil {
		return decideErr
	}
	if len(events) == 0 {
		return nil
	}
	return r.repo.Append(ctx, id, version, events)
}
