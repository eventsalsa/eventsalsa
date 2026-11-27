package persistence

import (
	"context"
	"errors"

	"eventsalsa.dev/cqrs/command"
)

// Decider binds a StateRepository to pure initial/evolve domain functions so
// any command decision function can be adapted into a command.Handler.
type Decider[S any, E any] struct {
	Repo    StateRepository[S]
	Initial func(id string) S
	Evolve  func(state S, event E) S
}

// NewCommandHandler builds a command.Handler for command C by loading state
// (or calling d.Initial when ErrNotFound), executing decide, folding events
// through d.Evolve, and saving the updated state cleanly.
func NewCommandHandler[S any, E any, C any](
	d Decider[S, E],
	idOf func(cmd C) string,
	decide func(state S, cmd C) ([]E, error),
) command.Func[C] {
	return func(ctx context.Context, cmd C) error {
		id := idOf(cmd)
		state, err := d.Repo.Load(ctx, id)
		if errors.Is(err, ErrNotFound) {
			state = d.Initial(id)
		} else if err != nil {
			return err
		}

		events, decideErr := decide(state, cmd)
		if decideErr != nil {
			return decideErr
		}
		if len(events) == 0 {
			return nil
		}

		for i := range events {
			state = d.Evolve(state, events[i])
		}
		return d.Repo.Save(ctx, state)
	}
}
