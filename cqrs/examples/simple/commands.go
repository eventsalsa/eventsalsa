package main

import (
	"context"
	"log/slog"

	"eventsalsa.dev/cqrs"
)

// PlaceOrder advances write-side state by registering a new order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// CancelOrder advances write-side state by canceling an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}

// PlaceOrderHandler implements command.Handler[PlaceOrder].
type PlaceOrderHandler struct {
	db     *appDB
	logger *slog.Logger
}

// Handle validates business rules and stores the new order.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd PlaceOrder) error {
	if cmd.TotalCents <= 0 {
		return errInvalidOrderTotal
	}
	h.logger.InfoContext(ctx, "placing order",
		"order_id", cmd.OrderID,
		"trace_id", cqrs.TraceIDFromContext(ctx),
		"command_id", cqrs.CommandIDFromContext(ctx),
	)
	return h.db.insertOrder(orderRecord{
		ID:         cmd.OrderID,
		CustomerID: cmd.CustomerID,
		Status:     statusPending,
		TotalCents: cmd.TotalCents,
	})
}

// CancelOrderHandler implements command.Handler[CancelOrder].
type CancelOrderHandler struct {
	db *appDB
}

// Handle transitions an existing order to CANCELED.
func (h *CancelOrderHandler) Handle(_ context.Context, cmd CancelOrder) error {
	return h.db.cancelOrder(cmd.OrderID)
}
