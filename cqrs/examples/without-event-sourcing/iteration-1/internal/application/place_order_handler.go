package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/domain"
)

// PlaceOrderHandler implements command.Handler[commands.PlaceOrder].
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
}

// Handle creates and saves a validated Order.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd commands.PlaceOrder) error {
	order, err := domain.NewOrder(cmd.OrderID, cmd.CustomerID, cmd.TotalCents)
	if err != nil {
		return err
	}
	return h.Orders.Save(ctx, order)
}
