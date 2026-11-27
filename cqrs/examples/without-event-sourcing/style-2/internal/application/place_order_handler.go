package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// PlaceOrderHandler implements command.Handler[domain.PlaceOrder],
// orchestrating persistence around the pure domain handler.
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle delegates order creation to the pure domain handler and persists the result.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd domain.PlaceOrder) error {
	order, err := domain.HandlePlaceOrder(cmd)
	if err != nil {
		return err
	}
	return h.Orders.Save(ctx, order)
}
