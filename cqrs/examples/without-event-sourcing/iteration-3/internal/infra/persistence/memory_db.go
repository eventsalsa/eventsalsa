package persistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	pbqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
)

// ErrTransientWrite simulates a retryable optimistic/serialization conflict.
var ErrTransientWrite = errors.New("transient database write conflict")

// MemoryDB simulates state-based SQL persistence implementing StateRepository[domain.Order]
// alongside direct presentation read queries (no event store).
type MemoryDB struct {
	orders        map[string]domain.Order
	customers     map[string]string
	mu            sync.RWMutex
	failFirstSave atomic.Bool
}

// NewMemoryDB constructs a pre-seeded in-memory database.
func NewMemoryDB() *MemoryDB {
	db := &MemoryDB{
		orders:    make(map[string]domain.Order),
		customers: map[string]string{"cust-42": "Ada Lovelace"},
	}
	db.failFirstSave.Store(true)
	return db
}

// Load retrieves stored Order decision state, returning ErrNotFound when absent.
func (db *MemoryDB) Load(_ context.Context, orderID string) (domain.Order, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	order, ok := db.orders[orderID]
	if !ok {
		return domain.Order{}, ErrNotFound
	}
	return order, nil
}

// Save stores folded Order state, failing once initially to exercise retries cleanly.
func (db *MemoryDB) Save(_ context.Context, order domain.Order) error {
	if db.failFirstSave.CompareAndSwap(true, false) {
		return ErrTransientWrite
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.orders[order.ID] = order
	return nil
}

// SelectOrderSummary projects a joined read view directly into the Protobuf response contract.
func (db *MemoryDB) SelectOrderSummary(orderID string) (*pbqueries.OrderSummary, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	order, ok := db.orders[orderID]
	if !ok {
		return nil, ErrNotFound
	}
	return &pbqueries.OrderSummary{
		OrderId:      order.ID,
		CustomerName: db.customers[order.CustomerID],
		Status:       order.Status,
		DisplayTotal: fmt.Sprintf("$%.2f", float64(order.TotalCents)/100),
	}, nil
}
