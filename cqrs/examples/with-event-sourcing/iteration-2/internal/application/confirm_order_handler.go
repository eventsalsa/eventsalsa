package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
)

// ConfirmOrderHandler implements command.Handler[ConfirmOrder].
type ConfirmOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle loads the Order aggregate, applies confirmation invariants, and saves changes.
func (h *ConfirmOrderHandler) Handle(ctx context.Context, cmd ConfirmOrder) error {
	order, err := h.Orders.Load(ctx, cmd.OrderID)
	if err != nil {
		return err
	}
	if confirmErr := order.Confirm(); confirmErr != nil {
		return confirmErr
	}
	return h.Orders.Save(ctx, order)
}
