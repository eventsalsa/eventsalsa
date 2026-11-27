package persistence

import (
	"context"
	"strings"

	"eventsalsa.dev/cqrs/command"
)

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

// Runner executes a command decision against loaded state for id and persists any
// resulting state change or emitted events atomically.
type Runner[S any, E any] interface {
	Run(ctx context.Context, id string, decide func(state S) ([]E, error)) error
}

// NewCommandHandler adapts a pure command decision function decide onto command.Handler[C]
// using runner for state reconstitution and persistence.
func NewCommandHandler[S any, E any, C any](
	runner Runner[S, E],
	idOf func(cmd C) string,
	decide func(state S, cmd C) ([]E, error),
) (command.Handler[C], error) {
	if isNilValue(runner) {
		return nil, ErrNilRunner
	}
	if idOf == nil {
		return nil, ErrNilTargetID
	}
	if decide == nil {
		return nil, ErrNilDecide
	}

	return command.Func[C](func(ctx context.Context, cmd C) error {
		id := idOf(cmd)
		if err := validateTarget(ctx, id); err != nil {
			return err
		}
		return runner.Run(ctx, id, func(state S) ([]E, error) {
			return decide(state, cmd)
		})
	}), nil
}

// MustNewCommandHandler calls NewCommandHandler and panics if any argument is invalid.
func MustNewCommandHandler[S any, E any, C any](
	runner Runner[S, E],
	idOf func(cmd C) string,
	decide func(state S, cmd C) ([]E, error),
) command.Handler[C] {
	h, err := NewCommandHandler(runner, idOf, decide)
	if err != nil {
		panic(err)
	}
	return h
}

func validateDependencies[S any, E any](repo any, decider Decider[S, E]) error {
	if isNilValue(repo) {
		return ErrNilRepository
	}
	return decider.validate()
}

func validateTarget(ctx context.Context, id string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if strings.TrimSpace(id) == "" {
		return ErrEmptyTargetID
	}
	return nil
}

func validateRun[S any, E any](ctx context.Context, id string, decide func(S) ([]E, error)) error {
	if err := validateTarget(ctx, id); err != nil {
		return err
	}
	if decide == nil {
		return ErrNilDecide
	}
	return nil
}
