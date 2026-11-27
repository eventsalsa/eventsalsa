package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
)

// PlaceOrderHandler implements command.Handler[commands.PlaceOrder].
type PlaceOrderHandler struct {
	Orders OrderEventStore
}

// Handle reconstitutes the order stream, evaluates placement rules, and appends emitted facts.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd commands.PlaceOrder) error {
	state, version, err := h.Orders.Load(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	events, decideErr := domain.Place(state, cmd.CustomerID, cmd.TotalCents)
	if decideErr != nil {
		return decideErr
	}
	if len(events) == 0 {
		return nil
	}
	return h.Orders.Append(ctx, cmd.OrderID, version, events)
}
