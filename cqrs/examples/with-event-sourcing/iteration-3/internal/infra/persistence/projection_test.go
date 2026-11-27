package persistence_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/persistence"
	"eventsalsa.dev/store"
)

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
