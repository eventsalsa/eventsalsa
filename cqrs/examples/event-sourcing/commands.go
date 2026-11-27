package main

import (
	"context"

	storepostgres "eventsalsa.dev/store/postgres"
)

// PlaceOrderHandler implements command.Handler[PlaceOrder].
type PlaceOrderHandler struct {
	store *storepostgres.Store
}

// Handle loads the order stream, evaluates decidePlaceOrder, and appends persisted facts.
func (h *PlaceOrderHandler) Handle(ctx context.Context, cmd PlaceOrder) error {
	return appendAndProject(ctx, h.store, cmd.OrderID, cmd, decidePlaceOrder)
}

// ConfirmOrderHandler implements command.Handler[ConfirmOrder].
type ConfirmOrderHandler struct {
	store *storepostgres.Store
}

// Handle loads the order stream, evaluates decideConfirmOrder, and appends persisted facts.
func (h *ConfirmOrderHandler) Handle(ctx context.Context, cmd ConfirmOrder) error {
	return appendAndProject(ctx, h.store, cmd.OrderID, cmd, decideConfirmOrder)
}
