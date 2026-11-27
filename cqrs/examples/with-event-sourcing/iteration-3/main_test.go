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
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain"
	domainevents "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/acl"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/queries"
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

func TestIteration3ACLAndDeciderFlow(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	repo := newStubEventRepository()
	repo.failNextConflicts.Store(1)

	var rejected []error
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
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
	if regErr := registerCommandHandlers(bus, repo); regErr != nil {
		t.Fatalf("registerCommandHandlers: %v", regErr)
	}

	// PlaceOrder recovers cleanly from the injected OCC conflict on retry.
	if dispatchErr := bus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); dispatchErr != nil {
		t.Fatalf("PlaceOrder: %v", dispatchErr)
	}
	if repo.appendCalls != 2 {
		t.Fatalf("appendCalls after OCC retry = %d, want 2", repo.appendCalls)
	}

	// Invalid amount, duplicate PlaceOrder, and unplaced ConfirmOrder are masked as domain rejections.
	if dispatchErr := bus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-invalid",
		CustomerId: "cust-42",
		TotalCents: 0,
	}); dispatchErr != nil {
		t.Fatalf("invalid PlaceOrder should mask rejection, got: %v", dispatchErr)
	}
	if dispatchErr := bus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); dispatchErr != nil {
		t.Fatalf("duplicate PlaceOrder should mask rejection, got: %v", dispatchErr)
	}
	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "missing"}); dispatchErr != nil {
		t.Fatalf("missing ConfirmOrder should mask rejection, got: %v", dispatchErr)
	}
	if len(rejected) != 3 ||
		!errors.Is(rejected[0], domain.ErrInvalidAmount) ||
		!errors.Is(rejected[1], domain.ErrAlreadyPlaced) ||
		!errors.Is(rejected[2], domain.ErrNotPlaced) {
		t.Fatalf("rejected = %v, want [ErrInvalidAmount, ErrAlreadyPlaced, ErrNotPlaced]", rejected)
	}

	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("ConfirmOrder: %v", dispatchErr)
	}
	// Second ConfirmOrder is an idempotent zero-event no-op leaving version at 2 without Append.
	callsBeforeNoop := repo.appendCalls
	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("idempotent ConfirmOrder: %v", dispatchErr)
	}
	if repo.appendCalls != callsBeforeNoop {
		t.Fatalf("idempotent ConfirmOrder called Append: got %d, want %d", repo.appendCalls, callsBeforeNoop)
	}

	history, ver, loadErr := repo.Load(ctx, "ord-1")
	if loadErr != nil || ver != 2 || len(history) != 2 {
		t.Fatalf("Load = (%v, %d, %v), want 2 events ver=2", history, ver, loadErr)
	}
	wantPlaced := domainevents.OrderPlaced{CustomerID: "cust-42", TotalCents: 2500}
	if placed, ok := history[0].(domainevents.OrderPlaced); !ok || placed != wantPlaced {
		t.Fatalf("history[0] = %#v, want %#v", history[0], wantPlaced)
	}
	if _, ok := history[1].(domainevents.OrderConfirmed); !ok {
		t.Fatalf("history[1] = %#v, want domainevents.OrderConfirmed", history[1])
	}

	if !isRetryableError(persistence.ErrConcurrencyConflict) || !isRetryableError(store.ErrOptimisticConcurrency) {
		t.Fatal("expected retry predicate to cover both persistence and store concurrency errors")
	}
	if isRetryableError(domain.ErrAlreadyPlaced) {
		t.Fatal("domain rejection must not be retried")
	}
	if !command.IsRejected(classifyDomainError(acl.ErrEmptyCommand)) {
		t.Fatal("ErrEmptyCommand must be classified as rejection")
	}
	if command.IsRejected(classifyDomainError(persistence.ErrConcurrencyConflict)) {
		t.Fatal("concurrency conflict must not be classified as rejection")
	}
}

func TestIteration3ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	repo := newStubEventRepository()
	decider := persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
	orders := persistence.FromEventRepository(repo, decider)

	placeACL := acl.NewPlaceOrderACL(persistence.MustNewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	))
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
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
}
