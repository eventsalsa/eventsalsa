package main

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/infra"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/queries"
	"eventsalsa.dev/cqrs/query"
)

func TestIteration1Example(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := infra.NewMemoryDB()

	var rejected []error
	cmdBus, err := newCommandBus(db, command.WithOnRejected(func(_ context.Context, _ any, rejErr error) {
		rejected = append(rejected, rejErr)
	}))
	if err != nil {
		t.Fatalf("newCommandBus: %v", err)
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}

	// Non-positive total is classified as a business rejection.
	if err := cmdBus.Dispatch(ctx, commands.PlaceOrder{
		OrderID:    "ord-invalid",
		CustomerID: "cust-42",
		TotalCents: 0,
	}); err != nil {
		t.Fatalf("invalid PlaceOrder should be masked rejection, got: %v", err)
	}

	// Canceling a non-existent order surfaces ErrOrderNotFound directly (unmasked).
	if err := cmdBus.Dispatch(ctx, commands.CancelOrder{
		OrderID: "missing",
		Reason:  "unknown",
	}); !errors.Is(err, domain.ErrOrderNotFound) {
		t.Fatalf("CancelOrder missing err = %v, want %v", err, domain.ErrOrderNotFound)
	}
	if _, err := query.Execute[queries.OrderSummary](ctx, qBus, queries.GetOrderSummary{OrderID: "missing"}); !errors.Is(
		err, domain.ErrOrderNotFound,
	) {
		t.Fatalf("GetOrderSummary missing err = %v, want %v", err, domain.ErrOrderNotFound)
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
	if len(rejected) != 2 || !errors.Is(rejected[0], domain.ErrInvalidOrderTotal) || !errors.Is(rejected[1], domain.ErrAlreadyCanceled) {
		t.Fatalf("rejected = %v, want [ErrInvalidOrderTotal, ErrAlreadyCanceled]", rejected)
	}

	canceled, err := query.Execute[queries.OrderSummary](ctx, qBus, queries.GetOrderSummary{OrderID: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
	if canceled.Status != domain.StatusCanceled {
		t.Fatalf("status = %q, want %q", canceled.Status, domain.StatusCanceled)
	}
}
