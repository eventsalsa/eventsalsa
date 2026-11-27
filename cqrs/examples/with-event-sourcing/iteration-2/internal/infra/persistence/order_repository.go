package persistence

import (
	"context"
	"errors"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

const streamTypeOrder = "Order"

// OrderRepository implements domain.OrderRepository using eventsalsa/store,
// eventmap-gen mappings, and an optional inline projection inside the write transaction.
type OrderRepository struct {
	Store      *storepostgres.Store
	Projection *OrderOverviewProjection
}

// Load reads the order stream from eventsalsa/store and reconstitutes Order via Evolve.
func (r *OrderRepository) Load(ctx context.Context, orderID string) (*domain.Order, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := r.Store.ReadStream(ctx, tx, streamTypeOrder, orderID, nil, nil)
	if err != nil {
		return nil, err
	}
	if len(stream.Events) == 0 {
		return nil, domain.ErrOrderNotFound
	}
	order := domain.ReconstituteOrder(orderID, stream.Version())
	for i := range stream.Events {
		domainEv, decErr := FromESEvent(stream.Events[i])
		if decErr != nil {
			return nil, decErr
		}
		order.Evolve(domainEv)
	}
	return order, nil
}

// Save maps uncommitted Order events via eventmap-gen, appends them optimistically to
// eventsalsa/store, triggers any configured inline projection atomically in tx, and marks
// committed events so Order matches the new stream version.
func (r *OrderRepository) Save(ctx context.Context, order *domain.Order) error {
	if order == nil {
		return nil
	}
	uncommitted := order.UncommittedEvents()
	if len(uncommitted) == 0 {
		return nil
	}

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

	toAppend, err := ToESEvents(streamTypeOrder, order.ID, uncommitted, opts...)
	if err != nil {
		return err
	}

	res, err := r.Store.Append(ctx, tx, store.Exact(order.Version()), toAppend)
	if err != nil {
		if order.Version() == 0 && errors.Is(err, store.ErrOptimisticConcurrency) {
			return domain.ErrAlreadyPlaced
		}
		return err
	}
	if r.Projection != nil {
		for i := range res.Events {
			if projErr := r.Projection.Handle(ctx, tx, res.Events[i]); projErr != nil {
				return projErr
			}
		}
	}
	order.MarkCommitted(res.ToVersion())
	return nil
}
