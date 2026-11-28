package bootstrap

import (
	"github.com/jackc/pgx/v5/pgxpool"

	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/queries"
	"eventsalsa.dev/cqrs/query"
)

// NewQueryBus builds the read-side query bus with observability middleware and registered handlers.
func NewQueryBus(pool *pgxpool.Pool) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infraqueries.GetOrderOverviewHandler{DB: pool}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
