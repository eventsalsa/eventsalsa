package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	statusPending  = "PENDING"
	statusCanceled = "CANCELED"
)

var (
	errOrderNotFound     = errors.New("order not found")
	errAlreadyCanceled   = errors.New("order already canceled")
	errInvalidOrderTotal = errors.New("order total must be positive")
	errTransientWrite    = errors.New("transient database write conflict")
)

type orderRecord struct {
	ID         string
	CustomerID string
	Status     string
	TotalCents int64
}

// appDB simulates an application relational database holding write-side order
// rows alongside customer lookup data for lightweight presentation reads.
type appDB struct {
	orders       map[string]orderRecord
	customers    map[string]string
	mu           sync.RWMutex
	failFirstPut atomic.Bool
}

func newAppDB() *appDB {
	db := &appDB{
		orders:    make(map[string]orderRecord),
		customers: map[string]string{"cust-42": "Ada Lovelace"},
	}
	db.failFirstPut.Store(true)
	return db
}

func (db *appDB) insertOrder(rec orderRecord) error {
	// Simulate one transient conflict so command.RetryMiddleware exercises cleanly.
	if db.failFirstPut.CompareAndSwap(true, false) {
		return errTransientWrite
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.orders[rec.ID] = rec
	return nil
}

func (db *appDB) cancelOrder(orderID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	rec, ok := db.orders[orderID]
	if !ok {
		return errOrderNotFound
	}
	if rec.Status == statusCanceled {
		return errAlreadyCanceled
	}
	rec.Status = statusCanceled
	db.orders[orderID] = rec
	return nil
}

func (db *appDB) selectOrderSummary(orderID string) (OrderSummary, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rec, ok := db.orders[orderID]
	if !ok {
		return OrderSummary{}, errOrderNotFound
	}
	return OrderSummary{
		OrderID:      rec.ID,
		CustomerName: db.customers[rec.CustomerID],
		Status:       rec.Status,
		DisplayTotal: fmt.Sprintf("$%.2f", float64(rec.TotalCents)/100),
	}, nil
}
