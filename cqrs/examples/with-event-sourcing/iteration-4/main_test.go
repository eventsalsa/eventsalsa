package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/config"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	domainevents "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/acl"
	infrapersistence "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
)

type stubEventRepository struct {
	mu                sync.RWMutex
	streams           map[string][]any
	appendCalls       int
	failNextConflicts atomic.Int32
}

func newStubEventRepository() *stubEventRepository {
	return &stubEventRepository{streams: make(map[string][]any)}
}

func (r *stubEventRepository) Load(_ context.Context, orderID string) (events []any, version int64, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	history := r.streams[orderID]
	return slices.Clone(history), int64(len(history)), nil
}

func (r *stubEventRepository) Append(_ context.Context, orderID string, expectedVersion int64, events []any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appendCalls++

	if r.failNextConflicts.Load() > 0 {
		r.failNextConflicts.Add(-1)
		return persistence.ErrConcurrencyConflict
	}

	current := r.streams[orderID]
	if int64(len(current)) != expectedVersion {
		return persistence.ErrConcurrencyConflict
	}
	r.streams[orderID] = append(slices.Clone(current), events...)
	return nil
}

func TestIteration4ACLAndDeciderFlow(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	repo := newStubEventRepository()
	repo.failNextConflicts.Store(1)

	var rejected []error
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: config.Default().Retry.MaxAttempts,
				RetryIf:     isRetryableError,
			}),
		),
		command.WithErrorClassifier(classifyDomainError),
		command.WithOnRejected(func(_ context.Context, _ any, rejErr error) {
			rejected = append(rejected, rejErr)
		}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	orders := persistence.FromEventRepository(repo, infrapersistence.OrderDecider())
	if regErr := registerCommandHandlers(bus, orders); regErr != nil {
		t.Fatalf("registerCommandHandlers: %v", regErr)
	}

	// PlaceOrder recovers cleanly from the injected OCC conflict on retry.
	if dispatchErr := bus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 10000,
	}); dispatchErr != nil {
		t.Fatalf("PlaceOrder: %v", dispatchErr)
	}
	if dispatchErr := bus.Dispatch(ctx, &commands.AddOrderItem{
		OrderId:   "ord-1",
		Sku:       "sku-salsa",
		UnitCents: 2500,
		Quantity:  2,
	}); dispatchErr != nil {
		t.Fatalf("AddOrderItem 1: %v", dispatchErr)
	}
	if dispatchErr := bus.Dispatch(ctx, &commands.AddOrderItem{
		OrderId:   "ord-1",
		Sku:       "sku-chips",
		UnitCents: 1900,
		Quantity:  1,
	}); dispatchErr != nil {
		t.Fatalf("AddOrderItem 2: %v", dispatchErr)
	}
	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("ConfirmOrder: %v", dispatchErr)
	}

	// Idempotent ConfirmOrder leaves stream untouched without Append.
	callsBeforeNoop := repo.appendCalls
	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("idempotent ConfirmOrder: %v", dispatchErr)
	}
	if repo.appendCalls != callsBeforeNoop {
		t.Fatalf("idempotent ConfirmOrder called Append: got %d, want %d", repo.appendCalls, callsBeforeNoop)
	}

	// Domain rejections are classified and masked cleanly.
	for _, cmd := range []any{
		&commands.PlaceOrder{OrderId: "ord-bad", CustomerId: "cust-42", TotalCents: 0},
		&commands.PlaceOrder{OrderId: "ord-1", CustomerId: "cust-42", TotalCents: 100},
		&commands.AddOrderItem{OrderId: "missing", Sku: "s", UnitCents: 10, Quantity: 1},
		&commands.AddOrderItem{OrderId: "ord-1", Sku: "s", UnitCents: 10, Quantity: 1},
	} {
		if dErr := bus.Dispatch(ctx, cmd); dErr != nil {
			t.Fatalf("expected masked rejection for %T, got %v", cmd, dErr)
		}
	}
	if len(rejected) != 4 ||
		!errors.Is(rejected[0], domain.ErrInvalidAmount) ||
		!errors.Is(rejected[1], domain.ErrAlreadyPlaced) ||
		!errors.Is(rejected[2], domain.ErrNotPlaced) ||
		!errors.Is(rejected[3], domain.ErrAlreadyConfirmed) {
		t.Fatalf("unexpected rejections: %v", rejected)
	}

	history, ver, loadErr := repo.Load(ctx, "ord-1")
	if loadErr != nil || ver != 4 || len(history) != 4 {
		t.Fatalf("Load = (%v, %d, %v), want 4 events ver=4", history, ver, loadErr)
	}
	state := domain.InitialOrder("ord-1")
	for _, ev := range history {
		state = domain.EvolveOrder(state, ev)
	}
	if state.TotalCents != 16900 || state.ItemCount != 3 || state.Version != 4 || !state.Confirmed {
		t.Fatalf("unexpected reconstituted state: %+v", state)
	}
	if _, ok := history[1].(domainevents.OrderItemAdded); !ok {
		t.Fatalf("history[1] = %#v, want OrderItemAdded", history[1])
	}

	if !isRetryableError(persistence.ErrConcurrencyConflict) || !isRetryableError(store.ErrOptimisticConcurrency) {
		t.Fatal("expected retry predicate to match concurrency errors")
	}
	if !command.IsRejected(classifyDomainError(acl.ErrEmptyCommand)) {
		t.Fatal("ErrEmptyCommand must be classified as rejection")
	}
	if command.IsRejected(classifyDomainError(persistence.ErrConcurrencyConflict)) {
		t.Fatal("concurrency conflict must not be classified as rejection")
	}
}

func TestIteration4ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	repo := newStubEventRepository()
	orders := persistence.FromEventRepository(repo, infrapersistence.OrderDecider())

	placeACL := acl.NewPlaceOrderACL(persistence.MustNewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	))
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	addItemACL := acl.NewAddOrderItemACL(persistence.MustNewCommandHandler(
		orders,
		func(cmd domain.AddOrderItem) string { return cmd.OrderID },
		domain.DecideAddOrderItem,
	))
	if err := addItemACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("addItemACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	confirmACL := acl.NewConfirmOrderACL(persistence.MustNewCommandHandler(
		orders,
		func(cmd domain.ConfirmOrder) string { return cmd.OrderID },
		domain.DecideConfirmOrder,
	))
	if err := confirmACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("confirmACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	qHandler := &infraqueries.GetOrderOverviewHandler{}
	if _, err := qHandler.Handle(ctx, &queries.GetOrderOverview{OrderId: "ord-1"}); !errors.Is(err, infraqueries.ErrMissingDB) {
		t.Fatalf("qHandler.Handle(nil DB) err = %v, want %v", err, infraqueries.ErrMissingDB)
	}
	if _, err := qHandler.Handle(ctx, nil); !errors.Is(err, infraqueries.ErrMissingDB) {
		t.Fatalf("qHandler.Handle(nil DB, nil q) err = %v, want %v", err, infraqueries.ErrMissingDB)
	}
	var typedNilHandler *infraqueries.GetOrderOverviewHandler
	if _, err := typedNilHandler.Handle(ctx, nil); !errors.Is(err, infraqueries.ErrMissingDB) {
		t.Fatalf("typedNilHandler.Handle err = %v, want %v", err, infraqueries.ErrMissingDB)
	}

	qBus, err := newQueryBus(nil)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}
	if _, err := query.Execute[*queries.OrderOverview](ctx, qBus, (*queries.GetOrderOverview)(nil)); !errors.Is(
		err, query.ErrNilQuery,
	) {
		t.Fatalf("Execute(nil) err = %v, want %v", err, query.ErrNilQuery)
	}

	if _, err := openApp(ctx, config.Config{}); !errors.Is(err, config.ErrBlankDatabaseURL) {
		t.Fatalf("openApp(zero cfg) err = %v, want %v", err, config.ErrBlankDatabaseURL)
	}
	if _, err := newCommandBus(config.Default().Retry, persistence.Adapter[domain.Order, any]{}); !errors.Is(
		err, persistence.ErrNilAdapter,
	) {
		t.Fatalf("newCommandBus(zero adapter) err = %v, want %v", err, persistence.ErrNilAdapter)
	}
}
