package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// PlaceOrderHandler implements command.Handler[PlaceOrder].
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle constructs a validated domain Order and persists it.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd PlaceOrder) error {
	order, err := domain.NewOrder(cmd.OrderID, cmd.CustomerID, cmd.TotalCents)
	if err != nil {
		return err
	}
	return h.Orders.Save(ctx, order)
}
