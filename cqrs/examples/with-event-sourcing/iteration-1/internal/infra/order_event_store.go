package infra

import (
	"context"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

const streamTypeOrder = "Order"

// OrderEventStore implements application.OrderEventStore using eventsalsa/store,
// eventmap-gen mappings, and an inline PostgreSQL read-model projection inside the tx.
type OrderEventStore struct {
	Store *storepostgres.Store
}

// Load reads the order stream from eventsalsa/store and folds it into OrderState.
func (s *OrderEventStore) Load(ctx context.Context, orderID string) (domain.OrderState, int64, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return domain.OrderState{}, 0, err
	}
	stream, err := s.Store.ReadStream(ctx, tx, streamTypeOrder, orderID, nil, nil)
	if err != nil {
		return domain.OrderState{}, 0, err
	}
	state := domain.InitialOrder(orderID)
	for i := range stream.Events {
		domainEv, decErr := FromESEvent(stream.Events[i])
		if decErr != nil {
			return domain.OrderState{}, 0, decErr
		}
		state = domain.EvolveOrder(state, domainEv)
	}
	return state, stream.Version(), nil
}

// Append maps domain events via eventmap-gen, appends them optimistically to eventsalsa/store,
// and updates order_overview_v1 atomically in the same transaction.
func (s *OrderEventStore) Append(
	ctx context.Context,
	orderID string,
	expectedVersion int64,
	domainEvents []any,
) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	var opts []Option
	if tid := cqrs.TraceIDFromContext(ctx); tid != "" {
		opts = append(opts, WithTraceID(tid))
	}
	if cid := cqrs.CommandIDFromContext(ctx); cid != "" {
		opts = append(opts, WithCausationID(cid))
	}

	toAppend, err := ToESEvents(streamTypeOrder, orderID, domainEvents, opts...)
	if err != nil {
		return err
	}

	expected := store.NoStream()
	if expectedVersion > 0 {
		expected = store.Exact(expectedVersion)
	}
	res, err := s.Store.Append(ctx, tx, expected, toAppend)
	if err != nil {
		return err
	}
	return projectInline(ctx, tx, res.Events)
}
