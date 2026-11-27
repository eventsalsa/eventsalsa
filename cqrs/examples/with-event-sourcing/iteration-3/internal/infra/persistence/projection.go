package persistence

import (
	"context"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"
	"eventsalsa.dev/store"
)

// OrderOverviewProjection maintains order_overview_v1 from persisted events.
//
// It implements the eventsalsa/projector Projection signature (Name + Handle(ctx, tx, event))
// so it can either run synchronously inline inside a write transaction (as wired here) or be
// registered directly with an asynchronous projector daemon without changing projection logic.
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
			INSERT INTO order_overview_v1 (order_id, customer_id, status, total_cents, version)
			VALUES ($1, $2, 'PLACED', $3, $4)
			ON CONFLICT (order_id) DO UPDATE
			SET customer_id = EXCLUDED.customer_id,
				status      = EXCLUDED.status,
				total_cents = EXCLUDED.total_cents,
				version     = EXCLUDED.version
			WHERE order_overview_v1.version < EXCLUDED.version
		`, ev.StreamID, d.CustomerID, d.TotalCents, ev.StreamVersion)
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
