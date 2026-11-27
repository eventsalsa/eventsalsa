package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/queries"
	"eventsalsa.dev/cqrs/query"
)

func TestStyle2Example(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := persistence.NewMemoryDB()

	cmdBus, err := newCommandBus(db, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("newCommandBus: %v", err)
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}

	if err := cmdBus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); err != nil {
		t.Fatalf("PlaceOrder: %v", err)
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

func TestStyle2ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	db := persistence.NewMemoryDB()

	placeACL := acl.NewPlaceOrderACL(&application.PlaceOrderHandler{
		Orders: db,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, domain.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, domain.ErrEmptyCommand)
	}

	cancelACL := acl.NewCancelOrderACL(&application.CancelOrderHandler{Orders: db})
	if err := cancelACL.Handle(ctx, nil); !errors.Is(err, domain.ErrEmptyCommand) {
		t.Fatalf("cancelACL(nil) err = %v, want %v", err, domain.ErrEmptyCommand)
	}

	qHandler := &infraqueries.GetOrderSummaryHandler{DB: db}
	if _, err := qHandler.Handle(ctx, nil); !errors.Is(err, domain.ErrEmptyQuery) {
		t.Fatalf("qHandler.Handle(nil) err = %v, want %v", err, domain.ErrEmptyQuery)
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
