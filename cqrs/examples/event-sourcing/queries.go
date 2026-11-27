package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetOrderOverview fetches a read-model row from order_overview_v1.
type GetOrderOverview struct {
	OrderID string
}

// OrderOverview is a projected read DTO queried independently of aggregate streams.
type OrderOverview struct {
	OrderID    string
	CustomerID string
	Status     string
	TotalCents int64
	Version    int64
}

// GetOrderOverviewHandler implements query.Handler[GetOrderOverview, OrderOverview].
type GetOrderOverviewHandler struct {
	db *pgxpool.Pool
}

// Handle issues a direct SQL query against the projected read table.
func (h *GetOrderOverviewHandler) Handle(ctx context.Context, q GetOrderOverview) (OrderOverview, error) {
	var view OrderOverview
	err := h.db.QueryRow(ctx, `
		SELECT order_id, customer_id, status, total_cents, version
		FROM order_overview_v1
		WHERE order_id = $1
	`, q.OrderID).Scan(&view.OrderID, &view.CustomerID, &view.Status, &view.TotalCents, &view.Version)
	return view, err
}
