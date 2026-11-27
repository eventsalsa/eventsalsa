package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/domain"
)

// CancelOrderHandler implements command.Handler[commands.CancelOrder].
type CancelOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle loads the order, applies cancellation invariants, and saves the updated state.
func (h *CancelOrderHandler) Handle(ctx context.Context, cmd commands.CancelOrder) error {
	order, err := h.Orders.Get(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	if cancelErr := order.Cancel(); cancelErr != nil {
		return cancelErr
	}
	return h.Orders.Save(ctx, order)
}
