package domain

import "context"

// OrderRepository defines write-side persistence operations required by domain use cases.
type OrderRepository interface {
	Load(ctx context.Context, orderID string) (*Order, error)
	Save(ctx context.Context, order *Order) error
}
