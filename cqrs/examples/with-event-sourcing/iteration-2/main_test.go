package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/application"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/bootstrap"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/acl"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/queries"
	"eventsalsa.dev/cqrs/query"
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
		return errors.New("version mismatch")
	}

	updated := append(append([]any(nil), existing...), uncommitted...)
	r.streams[order.ID] = updated
	order.MarkCommitted(int64(len(updated)))
	return nil
}

func TestIteration2ACLAndDomainFlow(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-trace")
	repo := newStubOrderRepository()

	var rejected []error
	bus, err := command.NewBus(
		command.WithMiddleware(command.ObservabilityMiddleware()),
		command.WithErrorClassifier(bootstrap.ClassifyDomainError),
		command.WithOnRejected(func(_ context.Context, _ any, rejErr error) {
			rejected = append(rejected, rejErr)
		}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	if regErr := bootstrap.RegisterCommandHandlers(bus, repo); regErr != nil {
		t.Fatalf("RegisterCommandHandlers: %v", regErr)
	}

	if dispatchErr := bus.Dispatch(ctx, &commands.PlaceOrder{
		OrderId:    "ord-1",
		CustomerId: "cust-42",
		TotalCents: 2500,
	}); dispatchErr != nil {
		t.Fatalf("PlaceOrder: %v", dispatchErr)
	}

	// Duplicate PlaceOrder and unknown ConfirmOrder are classified as domain rejections.
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
	if len(rejected) != 2 || !errors.Is(rejected[0], domain.ErrAlreadyPlaced) || !errors.Is(rejected[1], domain.ErrOrderNotFound) {
		t.Fatalf("rejected = %v, want [ErrAlreadyPlaced, ErrOrderNotFound]", rejected)
	}

	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("ConfirmOrder: %v", dispatchErr)
	}
	// Second ConfirmOrder is an idempotent no-op leaving version at 2.
	if dispatchErr := bus.Dispatch(ctx, &commands.ConfirmOrder{OrderId: "ord-1"}); dispatchErr != nil {
		t.Fatalf("idempotent ConfirmOrder: %v", dispatchErr)
	}

	reloaded, loadErr := repo.Load(ctx, "ord-1")
	if loadErr != nil {
		t.Fatalf("repo.Load: %v", loadErr)
	}
	if !reloaded.Placed || !reloaded.Confirmed || reloaded.Version() != 2 {
		t.Fatalf("unexpected reloaded order: placed=%v confirmed=%v version=%d",
			reloaded.Placed, reloaded.Confirmed, reloaded.Version())
	}
}

func TestIteration2ACLNilSafety(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "test-nil-trace")
	repo := newStubOrderRepository()

	placeACL := acl.NewPlaceOrderACL(&application.PlaceOrderHandler{Orders: repo})
	if err := placeACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("placeACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	confirmACL := acl.NewConfirmOrderACL(&application.ConfirmOrderHandler{Orders: repo})
	if err := confirmACL.Handle(ctx, nil); !errors.Is(err, acl.ErrEmptyCommand) {
		t.Fatalf("confirmACL(nil) err = %v, want %v", err, acl.ErrEmptyCommand)
	}

	qHandler := &infraqueries.GetOrderOverviewHandler{}
	if _, err := qHandler.Handle(ctx, nil); !errors.Is(err, infraqueries.ErrEmptyQuery) {
		t.Fatalf("qHandler.Handle(nil) err = %v, want %v", err, infraqueries.ErrEmptyQuery)
	}

	qBus, err := bootstrap.NewQueryBus(nil)
	if err != nil {
		t.Fatalf("NewQueryBus: %v", err)
	}
	if _, err := query.Execute[*queries.OrderOverview](ctx, qBus, (*queries.GetOrderOverview)(nil)); !errors.Is(
		err, query.ErrNilQuery,
	) {
		t.Fatalf("Execute(nil) err = %v, want %v", err, query.ErrNilQuery)
	}
}
