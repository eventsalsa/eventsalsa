package infra

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/queries"
)

// GetOrderOverviewHandler implements query.Handler[queries.GetOrderOverview, queries.OrderOverview].
type GetOrderOverviewHandler struct {
	DB *pgxpool.Pool
}

// Handle issues a direct SQL query against the projected order_overview_v1 table.
func (h *GetOrderOverviewHandler) Handle(
	ctx context.Context,
	q queries.GetOrderOverview,
) (queries.OrderOverview, error) {
	var view queries.OrderOverview
	err := h.DB.QueryRow(ctx, `
		SELECT order_id, customer_id, status, total_cents, version
		FROM order_overview_v1
		WHERE order_id = $1
	`, q.OrderID).Scan(&view.OrderID, &view.CustomerID, &view.Status, &view.TotalCents, &view.Version)
	return view, err
}
