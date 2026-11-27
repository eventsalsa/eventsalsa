package domain

import "context"

// OrderRepository defines write-side persistence operations required by use cases.
type OrderRepository interface {
	Save(ctx context.Context, order Order) error
	Get(ctx context.Context, orderID string) (Order, error)
}
