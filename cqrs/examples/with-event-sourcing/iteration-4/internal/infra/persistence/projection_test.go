package persistence_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
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

	for _, eventType := range []string{"OrderPlaced", "OrderItemAdded"} {
		if err := proj.Handle(context.Background(), nil, store.PersistedEvent{
			EventType:    eventType,
			EventVersion: 1,
			Payload:      []byte(`{broken-json`),
		}); err == nil {
			t.Fatalf("expected unmarshal error for invalid %s JSON payload", eventType)
		}
	}
}
