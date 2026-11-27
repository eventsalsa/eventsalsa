package main

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
	"eventsalsa.dev/cqrs/query"
)

func TestIteration3Example(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := persistence.NewMemoryDB()

	cmdBus, err := newCommandBus(db)
	if err != nil {
		t.Fatalf("newCommandBus: %v", err)
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}

	// Canceling before placement is masked as a domain rejection (ErrNotPlaced).
	if err := cmdBus.Dispatch(ctx, &commands.CancelOrder{
		OrderId: "ord-1",
		Reason:  "before place",
	}); err != nil {
		t.Fatalf("CancelOrder before place should be masked rejection, got: %v", err)
	}

	if err := cmdBus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}

	// Duplicate placement is masked as a domain rejection (ErrAlreadyPlaced).
	if err := cmdBus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); err != nil {
		t.Fatalf("duplicate PlaceOrder should be masked rejection, got: %v", err)
	}

	view, err := query.Execute[*queries.OrderSummary](ctx, qBus, &queries.GetOrderSummary{OrderId: "ord-1"})
	if err != nil {
		t.Fatalf("Execute GetOrderSummary: %v", err)
	}
	if view.GetCustomerName() != "Ada Lovelace" ||
		view.GetDisplayTotal() != "$25.00" ||
		view.GetStatus() != domain.StatusPending {
		t.Fatalf("unexpected view: %+v", view)
	}

	// Blank cancellation reason is masked as a domain rejection, keeping the order pending.
	if err := cmdBus.Dispatch(ctx, &commands.CancelOrder{OrderId: "ord-1", Reason: "   "}); err != nil {
		t.Fatalf("blank-reason CancelOrder should be masked rejection, got: %v", err)
	}
	stillPending, err := query.Execute[*queries.OrderSummary](ctx, qBus, &queries.GetOrderSummary{OrderId: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after blank-reason cancel: %v", err)
	}
	if stillPending.GetStatus() != domain.StatusPending {
		t.Fatalf("status = %q, want %q", stillPending.GetStatus(), domain.StatusPending)
	}

	if err := cmdBus.Dispatch(ctx, &commands.CancelOrder{OrderId: "ord-1", Reason: "test"}); err != nil {
		t.Fatalf("first CancelOrder: %v", err)
	}
	if err := cmdBus.Dispatch(ctx, &commands.CancelOrder{OrderId: "ord-1", Reason: "duplicate"}); err != nil {
		t.Fatalf("duplicate CancelOrder should be masked rejection, got: %v", err)
	}

	canceled, err := query.Execute[*queries.OrderSummary](ctx, qBus, &queries.GetOrderSummary{OrderId: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
	if canceled.GetStatus() != domain.StatusCanceled {
		t.Fatalf("status = %q, want %q", canceled.GetStatus(), domain.StatusCanceled)
	}
}

func TestIteration3ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	db := persistence.NewMemoryDB()

	placeACL := acl.NewPlaceOrderACL(application.NewDeciderHandler(
		db,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	))
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	cancelACL := acl.NewCancelOrderACL(application.NewDeciderHandler(
		db,
		func(cmd domain.CancelOrder) string { return cmd.OrderID },
		domain.DecideCancelOrder,
	))
	if err := cancelACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("cancelACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	qHandler := &infraqueries.GetOrderSummaryHandler{DB: db}
	if _, err := qHandler.Handle(ctx, nil); !errors.Is(err, infraqueries.ErrEmptyQuery) {
		t.Fatalf("qHandler.Handle(nil) err = %v, want %v", err, infraqueries.ErrEmptyQuery)
	}

	qBus, err := newQueryBus(db)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}
	if _, err := query.Execute[*queries.OrderSummary](ctx, qBus, (*queries.GetOrderSummary)(nil)); !errors.Is(
		err, query.ErrNilQuery,
	) {
		t.Fatalf("Execute(nil) err = %v, want %v", err, query.ErrNilQuery)
	}
}
