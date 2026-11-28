package persistence

import (
	"context"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"
	"eventsalsa.dev/store"
)

// OrderOverviewProjection maintains order_overview_v1 from persisted events.
type OrderOverviewProjection struct{}

// Name returns the projection name used for checkpoint tracking when run asynchronously.
func (*OrderOverviewProjection) Name() string {
	return "order_overview_v1"
}

// Handle applies a single persisted event to order_overview_v1 within tx.
//
//nolint:gocritic // hugeParam: matches projector.Projection signature
func (*OrderOverviewProjection) Handle(ctx context.Context, tx pgx.Tx, ev store.PersistedEvent) error {
	domainEv, err := FromESEvent(ev)
	if err != nil {
		return err
	}
	switch d := domainEv.(type) {
	case events.OrderPlaced:
		_, execErr := tx.Exec(ctx, `
			INSERT INTO order_overview_v1 (order_id, customer_id, status, total_cents, item_count, version)
			VALUES ($1, $2, 'PLACED', $3, 0, $4)
			ON CONFLICT (order_id) DO UPDATE
			SET customer_id = EXCLUDED.customer_id,
				status      = EXCLUDED.status,
				total_cents = EXCLUDED.total_cents,
				item_count  = EXCLUDED.item_count,
				version     = EXCLUDED.version
			WHERE order_overview_v1.version < EXCLUDED.version
		`, ev.StreamID, d.CustomerID, d.TotalCents, ev.StreamVersion)
		return execErr
	case events.OrderItemAdded:
		lineTotal := d.UnitCents * d.Quantity
		_, execErr := tx.Exec(ctx, `
			UPDATE order_overview_v1
			SET total_cents = order_overview_v1.total_cents + $2,
				item_count  = order_overview_v1.item_count + $3,
				version     = $4
			WHERE order_id = $1 AND version < $4
		`, ev.StreamID, lineTotal, d.Quantity, ev.StreamVersion)
		return execErr
	case events.OrderConfirmed:
		_, execErr := tx.Exec(ctx, `
			UPDATE order_overview_v1
			SET status = 'CONFIRMED', version = $2
			WHERE order_id = $1 AND version < $2
		`, ev.StreamID, ev.StreamVersion)
		return execErr
	default:
		return nil
	}
}
