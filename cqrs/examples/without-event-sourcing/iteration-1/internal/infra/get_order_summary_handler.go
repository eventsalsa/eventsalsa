package infra

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/queries"
)

// GetOrderSummaryHandler lives in infra as a thin read-side query handler
// returning public DTOs directly without touching domain write models.
type GetOrderSummaryHandler struct {
	DB *MemoryDB
}

// Handle fetches the summary DTO for one order.
func (h *GetOrderSummaryHandler) Handle(_ context.Context, q queries.GetOrderSummary) (queries.OrderSummary, error) {
	return h.DB.selectOrderSummary(q.OrderID)
}
