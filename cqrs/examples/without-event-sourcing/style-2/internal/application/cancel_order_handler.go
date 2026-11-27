package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// CancelOrderHandler implements command.Handler[domain.CancelOrder],
// orchestrating load/save around the pure domain handler.
type CancelOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle loads the order, applies the pure domain handler, and persists the updated state.
func (h *CancelOrderHandler) Handle(ctx context.Context, cmd domain.CancelOrder) error {
	order, err := h.Orders.Get(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	updated, cancelErr := domain.HandleCancelOrder(order, cmd)
	if cancelErr != nil {
		return cancelErr
	}
	return h.Orders.Save(ctx, updated)
}
