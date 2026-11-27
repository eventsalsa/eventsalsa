package queries

import (
	"context"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/infra/persistence"
	pbqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/queries"
)

// GetOrderSummaryHandler lives in infra as a thin read-side query handler
// returning public Protobuf contracts directly without touching domain write models.
type GetOrderSummaryHandler struct {
	DB *persistence.MemoryDB
}

// Handle fetches the summary Protobuf view for one order.
func (h *GetOrderSummaryHandler) Handle(
	_ context.Context,
	q *pbqueries.GetOrderSummary,
) (*pbqueries.OrderSummary, error) {
	if q == nil {
		return nil, domain.ErrEmptyQuery
	}
	return h.DB.SelectOrderSummary(q.GetOrderId())
}
