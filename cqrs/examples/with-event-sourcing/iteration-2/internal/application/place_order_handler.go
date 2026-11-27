package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
)

// PlaceOrderHandler implements command.Handler[PlaceOrder].
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle creates a new Order aggregate and persists it through the domain repository.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd PlaceOrder) error {
	order, err := domain.PlaceOrder(cmd.OrderID, cmd.CustomerID, cmd.TotalCents)
	if err != nil {
		return err
	}
	return h.Orders.Save(ctx, order)
}
