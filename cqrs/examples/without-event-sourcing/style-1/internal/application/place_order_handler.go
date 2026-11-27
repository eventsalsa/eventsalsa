package application

import (
	"context"
	"log/slog"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/domain"
)

// PlaceOrderHandler implements command.Handler[commands.PlaceOrder].
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
	Logger *slog.Logger
}

// Handle creates and saves a validated Order.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd commands.PlaceOrder) error {
	order, err := domain.NewOrder(cmd.OrderID, cmd.CustomerID, cmd.TotalCents)
	if err != nil {
		return err
	}
	h.Logger.InfoContext(ctx, "placing order",
		"order_id", order.ID,
		"trace_id", cqrs.TraceIDFromContext(ctx),
		"command_id", cqrs.CommandIDFromContext(ctx),
	)
	return h.Orders.Save(ctx, order)
}
