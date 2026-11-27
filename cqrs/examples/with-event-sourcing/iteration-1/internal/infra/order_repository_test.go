package infra_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/infra"
	"eventsalsa.dev/store"
)

func TestOrderRepository_TxGuardsAndNoOps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := &infra.OrderRepository{}

	if _, err := repo.Load(ctx, "ord-1"); !errors.Is(err, infra.ErrMissingTx) {
		t.Fatalf("Load without tx err = %v, want %v", err, infra.ErrMissingTx)
	}

	if err := repo.Save(ctx, nil); err != nil {
		t.Fatalf("Save(nil) should be no-op, got: %v", err)
	}

	clean := domain.ReconstituteOrder("ord-1", 1)
	if err := repo.Save(ctx, clean); err != nil {
		t.Fatalf("Save with zero uncommitted events should be no-op, got: %v", err)
	}

	dirty, err := domain.PlaceOrder("ord-1", "cust-42", 1200)
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if err := repo.Save(ctx, dirty); !errors.Is(err, infra.ErrMissingTx) {
		t.Fatalf("Save without tx err = %v, want %v", err, infra.ErrMissingTx)
	}
}

func TestOrderOverviewProjection_MetadataAndInvalidEvents(t *testing.T) {
	t.Parallel()

	proj := &infra.OrderOverviewProjection{}
	if proj.Name() != "order_overview_v1" {
		t.Fatalf("Name = %q, want order_overview_v1", proj.Name())
	}

	if err := proj.Handle(context.Background(), nil, store.PersistedEvent{
		EventType:    "UnknownEvent",
		EventVersion: 1,
		Payload:      []byte(`{}`),
	}); !errors.Is(err, store.ErrUnknownEvent) {
		t.Fatalf("unknown event err = %v, want %v", err, store.ErrUnknownEvent)
	}

	if err := proj.Handle(context.Background(), nil, store.PersistedEvent{
		EventType:    "OrderPlaced",
		EventVersion: 1,
		Payload:      []byte(`{broken-json`),
	}); err == nil {
		t.Fatal("expected unmarshal error for invalid JSON payload")
	}
}
