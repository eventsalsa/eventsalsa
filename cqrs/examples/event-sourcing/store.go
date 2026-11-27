package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

const (
	streamTypeOrder = "Order"
	eventPlaced     = "OrderPlaced"
	eventConfirmed  = "OrderConfirmed"
)

func loadOrderStream(
	ctx context.Context,
	tx pgx.Tx,
	es *storepostgres.Store,
	orderID string,
) (OrderState, store.ExpectedVersion, error) {
	stream, err := es.ReadStream(ctx, tx, streamTypeOrder, orderID, nil, nil)
	if err != nil {
		return OrderState{}, store.ExpectedVersion{}, err
	}
	state := initialOrder(orderID)
	for i := range stream.Events {
		ev := &stream.Events[i]
		domainEv, decErr := decodeDomainEvent(ev.EventType, ev.Payload)
		if decErr != nil {
			return OrderState{}, store.ExpectedVersion{}, decErr
		}
		state = evolveOrder(state, domainEv)
	}
	expected := store.NoStream()
	if stream.Version() > 0 {
		expected = store.Exact(stream.Version())
	}
	return state, expected, nil
}

func decodeDomainEvent(eventType string, payload []byte) (any, error) {
	switch eventType {
	case eventPlaced:
		var ev OrderPlaced
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	case eventConfirmed:
		var ev OrderConfirmed
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	default:
		return nil, fmt.Errorf("unknown event type %q", eventType)
	}
}

func encodeStoreEvents(ctx context.Context, orderID string, domainEvents []any) ([]store.Event, error) {
	obs, _ := cqrs.ObservabilityFromContext(ctx)
	traceID := store.NullString{String: obs.TraceID, Valid: obs.TraceID != ""}
	causationID := store.NullString{String: obs.CommandID, Valid: obs.CommandID != ""}

	out := make([]store.Event, 0, len(domainEvents))
	now := time.Now().UTC()
	for _, ev := range domainEvents {
		var eventType string
		switch ev.(type) {
		case OrderPlaced:
			eventType = eventPlaced
		case OrderConfirmed:
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

func appendAndProject[C any](
	ctx context.Context,
	es *storepostgres.Store,
	orderID string,
	cmd C,
	decide func(OrderState, C) ([]any, error),
) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	state, expected, err := loadOrderStream(ctx, tx, es, orderID)
	if err != nil {
		return err
	}
	domainEvents, err := decide(state, cmd)
	if err != nil {
		return err
	}
	if len(domainEvents) == 0 {
		return nil
	}
	toAppend, err := encodeStoreEvents(ctx, orderID, domainEvents)
	if err != nil {
		return err
	}
	res, err := es.Append(ctx, tx, expected, toAppend)
	if err != nil {
		return err
	}
	return projectInline(ctx, tx, res.Events)
}
