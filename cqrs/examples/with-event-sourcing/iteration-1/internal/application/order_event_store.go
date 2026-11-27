package application

import (
	"context"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
)

// OrderEventStore defines the stream load/append port needed by write-side use cases.
type OrderEventStore interface {
	Load(ctx context.Context, orderID string) (domain.Order, int64, error)
	Append(ctx context.Context, orderID string, expectedVersion int64, events []any) error
}
