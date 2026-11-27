package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
)

// ConfirmOrderHandler implements command.Handler[commands.ConfirmOrder].
type ConfirmOrderHandler struct {
	Orders OrderEventStore
}

// Handle reconstitutes the order stream, evaluates confirmation rules, and appends emitted facts.
func (h *ConfirmOrderHandler) Handle(ctx context.Context, cmd commands.ConfirmOrder) error {
	state, version, err := h.Orders.Load(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	events, decideErr := domain.Confirm(state)
	if decideErr != nil {
		return decideErr
	}
	if len(events) == 0 {
		return nil
	}
	return h.Orders.Append(ctx, cmd.OrderID, version, events)
}
