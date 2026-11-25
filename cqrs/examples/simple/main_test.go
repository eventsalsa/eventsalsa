package main

import (
	"context"
	"log/slog"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/query"
)

func TestSimpleCQRSExample(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := newRelationalDB()
	cmdBus, qBus, err := buildBuses(db, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("buildBuses: %v", err)
	}

	if err := cmdBus.Dispatch(ctx, PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	view, err := query.Execute[OrderSummary](ctx, qBus, GetOrderSummary{OrderID: "ord-1"})
	if err != nil {
		t.Fatalf("Execute GetOrderSummary: %v", err)
	}
	if view.CustomerName != "Ada Lovelace" || view.DisplayTotal != "$25.00" || view.Status != statusPending {
		t.Fatalf("unexpected view: %+v", view)
	}

	if err := cmdBus.Dispatch(ctx, CancelOrder{OrderID: "ord-1", Reason: "test"}); err != nil {
		t.Fatalf("first CancelOrder: %v", err)
	}
	if err := cmdBus.Dispatch(ctx, CancelOrder{OrderID: "ord-1", Reason: "duplicate"}); err != nil {
		t.Fatalf("duplicate CancelOrder should be masked rejection, got: %v", err)
	}

	canceled, err := query.Execute[OrderSummary](ctx, qBus, GetOrderSummary{OrderID: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
	if canceled.Status != statusCanceled {
		t.Fatalf("status = %q, want %q", canceled.Status, statusCanceled)
	}
}
