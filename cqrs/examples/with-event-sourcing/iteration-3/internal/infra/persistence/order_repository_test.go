package persistence_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/persistence"
	"eventsalsa.dev/store"
)

func TestOrderRepository_TxGuardsAndEmptyAppendNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := &persistence.OrderRepository{}

	if _, _, err := repo.Load(ctx, "ord-1"); !errors.Is(err, persistence.ErrMissingTx) {
		t.Fatalf("Load without tx err = %v, want %v", err, persistence.ErrMissingTx)
	}

	if err := repo.Append(ctx, "ord-1", 0, nil); err != nil {
		t.Fatalf("Append(nil) should be no-op, got: %v", err)
	}

	if err := repo.Append(ctx, "ord-1", 0, []any{events.OrderPlaced{
		CustomerID: "cust-42",
		TotalCents: 1200,
	}}); !errors.Is(err, persistence.ErrMissingTx) {
		t.Fatalf("Append without tx err = %v, want %v", err, persistence.ErrMissingTx)
	}
}

func TestOrderOverviewProjection_MetadataAndInvalidEvents(t *testing.T) {
	t.Parallel()

	proj := &persistence.OrderOverviewProjection{}
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
