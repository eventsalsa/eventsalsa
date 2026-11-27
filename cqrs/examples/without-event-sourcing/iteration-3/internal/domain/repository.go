package domain

import "context"

// OrderRepository persists current Order decision state without an event log.
type OrderRepository interface {
	Find(ctx context.Context, orderID string) (Order, bool, error)
	Save(ctx context.Context, order Order) error
}
