package persistence

import (
	"context"
	"errors"
)

// StateRepository persists folded aggregate state S without an underlying event log.
// Load must return ErrNotFound when no record exists yet for id.
type StateRepository[S any] interface {
	Load(ctx context.Context, id string) (S, error)
	Save(ctx context.Context, state S) error
}

// StateRunner binds a StateRepository to a Decider so command decision functions can
// operate over loaded state, fold emitted facts via Evolve, and save updated state cleanly.
type StateRunner[S any, E any] struct {
	repo    StateRepository[S]
	decider Decider[S, E]
}

// NewStateRunner constructs a validated StateRunner backed by repo and decider.
func NewStateRunner[S any, E any](repo StateRepository[S], decider Decider[S, E]) (*StateRunner[S, E], error) {
	if err := validateDependencies(repo, decider); err != nil {
		return nil, err
	}
	return &StateRunner[S, E]{
		repo:    repo,
		decider: decider,
	}, nil
}

// MustNewStateRunner calls NewStateRunner and panics if any dependency is invalid.
func MustNewStateRunner[S any, E any](repo StateRepository[S], decider Decider[S, E]) *StateRunner[S, E] {
	runner, err := NewStateRunner(repo, decider)
	if err != nil {
		panic(err)
	}
	return runner
}

// Run loads state for id (falling back to Decider.Initial when ErrNotFound), invokes decide,
// folds any emitted events through Decider.Evolve, and persists updated state via repo.Save.
// When decide returns zero events without an error, Save is skipped as an idempotent no-op.
func (r *StateRunner[S, E]) Run(ctx context.Context, id string, decide func(state S) ([]E, error)) error {
	if r == nil {
		return ErrNilRunner
	}
	if err := validateRun(ctx, id, decide); err != nil {
		return err
	}

	state, err := r.repo.Load(ctx, id)
	if errors.Is(err, ErrNotFound) {
		state = r.decider.Initial(id)
	} else if err != nil {
		return err
	}

	events, decideErr := decide(state)
	if decideErr != nil {
		return decideErr
	}
	if len(events) == 0 {
		return nil
	}

	for i := range events {
		state = r.decider.Evolve(state, events[i])
	}
	return r.repo.Save(ctx, state)
}
