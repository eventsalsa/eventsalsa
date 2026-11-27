package persistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	pbqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
	cqrspersistence "eventsalsa.dev/cqrs/persistence"
)

// ErrTransientWrite simulates a retryable storage fault.
var ErrTransientWrite = errors.New("transient database write conflict")

type storedOrder struct {
	order   domain.Order
	version int64
}

// MemoryDB simulates state-based SQL persistence implementing cqrspersistence.StateRepository[domain.Order]
// with optimistic concurrency control alongside direct presentation read queries (no event store).
type MemoryDB struct {
	orders        map[string]storedOrder
	customers     map[string]string
	mu            sync.RWMutex
	failFirstSave atomic.Bool
}

// NewMemoryDB constructs a pre-seeded in-memory database.
func NewMemoryDB() *MemoryDB {
	db := &MemoryDB{
		orders:    make(map[string]storedOrder),
		customers: map[string]string{"cust-42": "Ada Lovelace"},
	}
	db.failFirstSave.Store(true)
	return db
}

// Load retrieves stored Order decision state and version, returning cqrspersistence.ErrNotFound when absent.
func (db *MemoryDB) Load(_ context.Context, orderID string) (domain.Order, int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rec, ok := db.orders[orderID]
	if !ok {
		return domain.Order{}, 0, cqrspersistence.ErrNotFound
	}
	return rec.order, rec.version, nil
}

// Save stores folded Order state guarded by expectedVersion OCC, failing once initially to exercise retries.
func (db *MemoryDB) Save(_ context.Context, orderID string, expectedVersion int64, order domain.Order) error {
	if db.failFirstSave.CompareAndSwap(true, false) {
		return ErrTransientWrite
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	rec, exists := db.orders[orderID]
	if expectedVersion == 0 && exists {
		return cqrspersistence.ErrConcurrencyConflict
	}
	if expectedVersion > 0 && (!exists || rec.version != expectedVersion) {
		return cqrspersistence.ErrConcurrencyConflict
	}

	db.orders[orderID] = storedOrder{
		order:   order,
		version: expectedVersion + 1,
	}
	return nil
}

// SelectOrderSummary projects a joined read view directly into the Protobuf response contract.
func (db *MemoryDB) SelectOrderSummary(orderID string) (*pbqueries.OrderSummary, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rec, ok := db.orders[orderID]
	if !ok {
		return nil, cqrspersistence.ErrNotFound
	}
	return &pbqueries.OrderSummary{
		OrderId:      rec.order.ID,
		CustomerName: db.customers[rec.order.CustomerID],
		Status:       rec.order.Status,
		DisplayTotal: fmt.Sprintf("$%.2f", float64(rec.order.TotalCents)/100),
	}, nil
}
