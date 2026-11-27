package application

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
)

// NewDeciderHandler builds a command.Handler for any domain command C using pure
// InitialOrder / Decide / EvolveOrder functions over state persistence (no event log).
func NewDeciderHandler[C any](
	repo domain.OrderRepository,
	idOf func(cmd C) string,
	decide func(state domain.Order, cmd C) ([]any, error),
) command.Func[C] {
	return func(ctx context.Context, cmd C) error {
		id := idOf(cmd)
		state, found, err := repo.Find(ctx, id)
		if err != nil {
			return err
		}
		if !found {
			state = domain.InitialOrder(id)
		}

		events, decideErr := decide(state, cmd)
		if decideErr != nil {
			return decideErr
		}
		if len(events) == 0 {
			return nil
		}

		for i := range events {
			state = domain.EvolveOrder(state, events[i])
		}
		return repo.Save(ctx, state)
	}
}
