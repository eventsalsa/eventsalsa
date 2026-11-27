package main

import (
	"context"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/infra"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/queries"
	"eventsalsa.dev/cqrs/query"
)

func TestStyle1Example(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := infra.NewMemoryDB()

	cmdBus, err := newCommandBus(db)
	if err != nil {
		t.Fatalf("newCommandBus: %v", err)
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}

	if err := cmdBus.Dispatch(ctx, commands.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	view, err := query.Execute[queries.OrderSummary](ctx, qBus, queries.GetOrderSummary{OrderID: "ord-1"})
	if err != nil {
		t.Fatalf("Execute GetOrderSummary: %v", err)
	}
	if view.CustomerName != "Ada Lovelace" || view.DisplayTotal != "$25.00" || view.Status != domain.StatusPending {
		t.Fatalf("unexpected view: %+v", view)
	}

	if err := cmdBus.Dispatch(ctx, commands.CancelOrder{OrderID: "ord-1", Reason: "test"}); err != nil {
		t.Fatalf("first CancelOrder: %v", err)
	}
	if err := cmdBus.Dispatch(ctx, commands.CancelOrder{OrderID: "ord-1", Reason: "duplicate"}); err != nil {
		t.Fatalf("duplicate CancelOrder should be masked rejection, got: %v", err)
	}

	canceled, err := query.Execute[queries.OrderSummary](ctx, qBus, queries.GetOrderSummary{OrderID: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
	if canceled.Status != domain.StatusCanceled {
		t.Fatalf("status = %q, want %q", canceled.Status, domain.StatusCanceled)
	}
}
