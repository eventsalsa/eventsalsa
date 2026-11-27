package infra

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/store"
)

const (
	streamTypeOrder = "Order"
	eventPlaced     = "OrderPlaced"
	eventConfirmed  = "OrderConfirmed"
)

func decodeDomainEvent(eventType string, payload []byte) (any, error) {
	switch eventType {
	case eventPlaced:
		var ev domain.OrderPlaced
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	case eventConfirmed:
		var ev domain.OrderConfirmed
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	default:
		return nil, fmt.Errorf("unknown event type %q", eventType)
	}
}

func encodeStoreEvents(ctx context.Context, orderID string, domainEvents []any) ([]store.Event, error) {
	rawTraceID := cqrs.TraceIDFromContext(ctx)
	rawCommandID := cqrs.CommandIDFromContext(ctx)
	traceID := store.NullString{String: rawTraceID, Valid: rawTraceID != ""}
	causationID := store.NullString{String: rawCommandID, Valid: rawCommandID != ""}

	out := make([]store.Event, 0, len(domainEvents))
	now := time.Now().UTC()
	for _, ev := range domainEvents {
		var eventType string
		switch ev.(type) {
		case domain.OrderPlaced:
			eventType = eventPlaced
		case domain.OrderConfirmed:
			eventType = eventConfirmed
		default:
			return nil, fmt.Errorf("unsupported domain event %T", ev)
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Event{
			StreamType:   streamTypeOrder,
			StreamID:     orderID,
			EventID:      uuid.New(),
			EventType:    eventType,
			EventVersion: 1,
			Payload:      payload,
			Metadata:     []byte(`{}`),
			TraceID:      traceID,
			CausationID:  causationID,
			CreatedAt:    now,
		})
	}
	return out, nil
}
