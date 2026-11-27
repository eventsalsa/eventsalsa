package infra

import (
	"context"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain/events"
	"eventsalsa.dev/store"
)

// projectInline updates order_overview_v1 inside the active write transaction.
// When projecting asynchronously instead, implement projector.Projection from eventsalsa/projector.
func projectInline(ctx context.Context, tx pgx.Tx, persisted []store.PersistedEvent) error {
	for i := range persisted {
		ev := &persisted[i]
		domainEv, err := FromESEvent(*ev)
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
			if execErr != nil {
				return execErr
			}
		case events.OrderConfirmed:
			_, execErr := tx.Exec(ctx, `
				UPDATE order_overview_v1
				SET status = 'CONFIRMED', version = $2
				WHERE order_id = $1 AND version < $2
			`, ev.StreamID, ev.StreamVersion)
			if execErr != nil {
				return execErr
			}
		}
	}
	return nil
}
