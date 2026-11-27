package application

import (
	"context"
	"log/slog"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// PlaceOrderHandler implements command.Handler[domain.PlaceOrder].
type PlaceOrderHandler struct {
	Orders domain.OrderRepository
	Logger *slog.Logger
}

// Handle creates and saves a validated Order from a domain command.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd domain.PlaceOrder) error {
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
