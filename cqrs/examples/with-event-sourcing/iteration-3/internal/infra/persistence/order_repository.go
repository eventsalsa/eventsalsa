package persistence

import (
	"context"
	"errors"
	"fmt"

	"eventsalsa.dev/cqrs"
	cqrspersistence "eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

const streamTypeOrder = "Order"

// OrderRepository implements cqrspersistence.EventRepository[any] over eventsalsa/store,
// eventmap-gen mappings, and an optional inline projection inside the write transaction.
type OrderRepository struct {
	Store      *storepostgres.Store
	Projection *OrderOverviewProjection
}

// Load reads the persisted order event stream from eventsalsa/store and returns decoded
// domain events alongside the current stream version (0 when no stream exists yet).
func (r *OrderRepository) Load(ctx context.Context, orderID string) (events []any, version int64, err error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	stream, err := r.Store.ReadStream(ctx, tx, streamTypeOrder, orderID, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	if len(stream.Events) == 0 {
		return nil, 0, nil
	}
	domainEvents, err := FromESEvents[any](stream.Events)
	if err != nil {
		return nil, 0, err
	}
	return domainEvents, stream.Version(), nil
}

// Append maps domainEvents via eventmap-gen, appends them optimistically to eventsalsa/store
// at expectedVersion (translating store.ErrOptimisticConcurrency to
// cqrspersistence.ErrConcurrencyConflict), and executes any configured inline projection.
func (r *OrderRepository) Append(
	ctx context.Context,
	orderID string,
	expectedVersion int64,
	domainEvents []any,
) error {
	if len(domainEvents) == 0 {
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

	toAppend, err := ToESEvents(streamTypeOrder, orderID, domainEvents, opts...)
	if err != nil {
		return err
	}

	res, err := r.Store.Append(ctx, tx, store.Exact(expectedVersion), toAppend)
	if err != nil {
		if errors.Is(err, store.ErrOptimisticConcurrency) {
			return fmt.Errorf("%w: %w", cqrspersistence.ErrConcurrencyConflict, err)
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
	return nil
}
