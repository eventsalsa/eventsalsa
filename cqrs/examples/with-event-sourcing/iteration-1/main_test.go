package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/queries"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
)

type stubOrderRepository struct {
	mu      sync.RWMutex
	streams map[string][]any
}

func newStubOrderRepository() *stubOrderRepository {
	return &stubOrderRepository{streams: make(map[string][]any)}
}

func (r *stubOrderRepository) Load(_ context.Context, orderID string) (*domain.Order, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	history := r.streams[orderID]
	if len(history) == 0 {
		return nil, domain.ErrOrderNotFound
	}
	order := domain.ReconstituteOrder(orderID, int64(len(history)))
	for i := range history {
		order.Evolve(history[i])
	}
	return order, nil
}

func (r *stubOrderRepository) Save(_ context.Context, order *domain.Order) error {
	if order == nil {
		return nil
	}
	uncommitted := order.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existing := r.streams[order.ID]
	if order.Version() == 0 && len(existing) > 0 {
		return domain.ErrAlreadyPlaced
	}
	if int64(len(existing)) != order.Version() {
		return store.ErrOptimisticConcurrency
	}

	updated := append(append([]any(nil), existing...), uncommitted...)
	r.streams[order.ID] = updated
	order.MarkCommitted(int64(len(updated)))
	return nil
}

func TestIteration1ApplicationAndDomainFlow(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	repo := newStubOrderRepository()

	var rejected []error
	bus, err := command.NewBus(
		command.WithMiddleware(command.ObservabilityMiddleware()),
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

	// Non-positive amount and unplaced ConfirmOrder are rejected cleanly.
	if dErr := bus.Dispatch(ctx, commands.PlaceOrder{
		OrderID:    "ord-invalid",
		CustomerID: "cust-42",
		TotalCents: 0,
	}); dErr != nil {
		t.Fatalf("invalid PlaceOrder should be masked rejection, got: %v", dErr)
	}
	if dErr := bus.Dispatch(ctx, commands.ConfirmOrder{OrderID: "missing"}); dErr != nil {
		t.Fatalf("missing ConfirmOrder should be masked rejection, got: %v", dErr)
	}

	if dErr := bus.Dispatch(ctx, commands.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}); dErr != nil {
		t.Fatalf("PlaceOrder: %v", dErr)
	}
	if dErr := bus.Dispatch(ctx, commands.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}); dErr != nil {
		t.Fatalf("duplicate PlaceOrder should be masked rejection, got: %v", dErr)
	}

	if len(rejected) != 3 ||
		!errors.Is(rejected[0], domain.ErrInvalidAmount) ||
		!errors.Is(rejected[1], domain.ErrOrderNotFound) ||
		!errors.Is(rejected[2], domain.ErrAlreadyPlaced) {
		t.Fatalf("rejected = %v, want [ErrInvalidAmount, ErrOrderNotFound, ErrAlreadyPlaced]", rejected)
	}

	if dErr := bus.Dispatch(ctx, commands.ConfirmOrder{OrderID: "ord-1"}); dErr != nil {
		t.Fatalf("ConfirmOrder: %v", dErr)
	}
	if dErr := bus.Dispatch(ctx, commands.ConfirmOrder{OrderID: "ord-1"}); dErr != nil {
		t.Fatalf("idempotent ConfirmOrder: %v", dErr)
	}

	reloaded, loadErr := repo.Load(ctx, "ord-1")
	if loadErr != nil {
		t.Fatalf("repo.Load: %v", loadErr)
	}
	if !reloaded.Placed ||
		!reloaded.Confirmed ||
		reloaded.CustomerID != "cust-42" ||
		reloaded.TotalCents != 2500 ||
		reloaded.Version() != 2 {
		t.Fatalf("unexpected reloaded order: %+v version=%d", reloaded, reloaded.Version())
	}

	if command.IsRejected(classifyDomainError(store.ErrOptimisticConcurrency)) {
		t.Fatal("optimistic concurrency error must not be classified as rejection")
	}
}

func TestIteration1QueryBoundaryGuards(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-query-trace")
	qBus, err := newQueryBus(nil)
	if err != nil {
		t.Fatalf("newQueryBus: %v", err)
	}
	type unknownQuery struct{}
	if _, err := query.Execute[queries.OrderOverview](ctx, qBus, unknownQuery{}); !errors.Is(
		err, cqrs.ErrNoHandler,
	) {
		t.Fatalf("Execute(unknownQuery) err = %v, want %v", err, cqrs.ErrNoHandler)
	}
}
