package queries

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	pbqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/queries"
)

// GetOrderOverviewHandler lives in infra as a thin read-side SQL query handler
// returning public Protobuf contracts directly without touching domain write models.
type GetOrderOverviewHandler struct {
	DB *pgxpool.Pool
}

// Handle issues a direct SQL query against the projected order_overview_v1 table.
func (h *GetOrderOverviewHandler) Handle(
	ctx context.Context,
	q *pbqueries.GetOrderOverview,
) (*pbqueries.OrderOverview, error) {
	if q == nil {
		return nil, ErrEmptyQuery
	}
	var view pbqueries.OrderOverview
	err := h.DB.QueryRow(ctx, `
		SELECT order_id, customer_id, status, total_cents, version
		FROM order_overview_v1
		WHERE order_id = $1
	`, q.GetOrderId()).Scan(&view.OrderId, &view.CustomerId, &view.Status, &view.TotalCents, &view.Version)
	if err != nil {
		return nil, err
	}
	return &view, nil
}
