package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// CancelOrderHandler implements command.Handler[CancelOrder].
type CancelOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle loads the order, enforces cancellation invariants on the entity, and persists the update.
func (h *CancelOrderHandler) Handle(ctx context.Context, cmd CancelOrder) error {
	order, err := h.Orders.Get(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	if cancelErr := order.Cancel(cmd.Reason); cancelErr != nil {
		return cancelErr
	}
	return h.Orders.Save(ctx, order)
}
