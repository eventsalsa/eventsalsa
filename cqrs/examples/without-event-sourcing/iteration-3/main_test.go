package main

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/acl"
	infrapersistence "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/query"
)

func TestIteration3Example(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	db := infrapersistence.NewMemoryDB()

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

	// Missing read view returns persistence.ErrNotFound.
	if _, err := query.Execute[*queries.OrderSummary](ctx, qBus, &queries.GetOrderSummary{OrderId: "missing"}); !errors.Is(
		err, persistence.ErrNotFound,
	) {
		t.Fatalf("missing GetOrderSummary err = %v, want %v", err, persistence.ErrNotFound)
	}

	// Canceling before placement and invalid order totals are rejected cleanly.
	if err := cmdBus.Dispatch(ctx, &commands.CancelOrder{
		OrderId: "ord-1",
		Reason:  "before place",
	}); err != nil {
		t.Fatalf("CancelOrder before place should be masked rejection, got: %v", err)
	}
	if err := cmdBus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-invalid",
		CustomerId: "cust-42",
		TotalCents: 0,
	}); err != nil {
		t.Fatalf("invalid PlaceOrder should be masked rejection, got: %v", err)
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

	if len(rejected) != 5 ||
		!errors.Is(rejected[0], domain.ErrNotPlaced) ||
		!errors.Is(rejected[1], domain.ErrInvalidOrderTotal) ||
		!errors.Is(rejected[2], domain.ErrAlreadyPlaced) ||
		!errors.Is(rejected[3], domain.ErrCancellationReasonRequired) ||
		!errors.Is(rejected[4], domain.ErrAlreadyCanceled) {
		t.Fatalf("unexpected rejected sequence: %v", rejected)
	}

	canceled, err := query.Execute[*queries.OrderSummary](ctx, qBus, &queries.GetOrderSummary{OrderId: "ord-1"})
	if err != nil {
		t.Fatalf("Execute after cancel: %v", err)
	}
	if canceled.GetStatus() != domain.StatusCanceled {
		t.Fatalf("status = %q, want %q", canceled.GetStatus(), domain.StatusCanceled)
	}

	// All MemoryDB.Save OCC conflict branches fail cleanly with ErrConcurrencyConflict.
	_, currentVer, loadErr := db.Load(ctx, "ord-1")
	if loadErr != nil || currentVer != 2 {
		t.Fatalf("Load = (ver=%d, err=%v), want version 2", currentVer, loadErr)
	}
	for _, tc := range []struct {
		name     string
		id       string
		expected int64
	}{
		{name: "expected zero on existing record", id: "ord-1", expected: 0},
		{name: "stale version on existing record", id: "ord-1", expected: 1},
		{name: "positive expected version on missing record", id: "missing", expected: 1},
	} {
		if saveErr := db.Save(ctx, tc.id, tc.expected, domain.Order{ID: tc.id}); !errors.Is(
			saveErr, persistence.ErrConcurrencyConflict,
		) {
			t.Fatalf("%s Save err = %v, want %v", tc.name, saveErr, persistence.ErrConcurrencyConflict)
		}
	}

	// Retry policy covers both transient write faults and OCC conflicts; non-domain faults pass unmasked.
	if !isRetryableError(infrapersistence.ErrTransientWrite) || !isRetryableError(persistence.ErrConcurrencyConflict) {
		t.Fatal("expected retry predicate to match transient and OCC conflict errors")
	}
	if isRetryableError(domain.ErrAlreadyPlaced) {
		t.Fatal("domain rejection must not be marked retryable")
	}
	if command.IsRejected(classifyDomainError(persistence.ErrConcurrencyConflict)) {
		t.Fatal("concurrency conflict must not be classified as a domain rejection")
	}
}

func TestIteration3ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	db := infrapersistence.NewMemoryDB()

	decider := persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
	orders := persistence.FromStateRepository(db, decider)

	placeACL := acl.NewPlaceOrderACL(persistence.MustNewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	))
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	cancelACL := acl.NewCancelOrderACL(persistence.MustNewCommandHandler(
		orders,
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
