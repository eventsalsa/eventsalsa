package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// CancelOrderHandler implements command.Handler[domain.CancelOrder].
type CancelOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle loads the order, applies cancellation invariants, and persists the update.
func (h *CancelOrderHandler) Handle(ctx context.Context, cmd domain.CancelOrder) error {
	order, err := h.Orders.Get(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	if cancelErr := order.Cancel(); cancelErr != nil {
		return cancelErr
	}
	return h.Orders.Save(ctx, order)
}
