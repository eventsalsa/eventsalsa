// Package main demonstrates Simple CQRS without Event Sourcing:
// write-side commands enforce invariants on authoritative state while
// read-side queries return presentation-shaped DTOs directly.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/query"
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

// PlaceOrder advances write-side state by registering a new order.
type PlaceOrder struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// CancelOrder advances write-side state by canceling an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}

// GetOrderSummary fetches a display DTO for screens or API responses.
type GetOrderSummary struct {
	OrderID string
}

// OrderSummary is a presentation read model shaped specifically for callers.
type OrderSummary struct {
	OrderID      string
	CustomerName string
	Status       string
	DisplayTotal string
}

type orderRecord struct {
	ID         string
	CustomerID string
	Status     string
	TotalCents int64
}

// relationalDB simulates a single shared PostgreSQL database holding normalized
// write tables alongside denormalized/joined customer display lookup data.
type relationalDB struct {
	orders       map[string]orderRecord
	customers    map[string]string
	mu           sync.RWMutex
	failFirstPut atomic.Bool
}

func newRelationalDB() *relationalDB {
	db := &relationalDB{
		orders:    make(map[string]orderRecord),
		customers: map[string]string{"cust-42": "Ada Lovelace"},
	}
	db.failFirstPut.Store(true)
	return db
}

func (db *relationalDB) insertOrder(rec orderRecord) error {
	// Simulate one transient write conflict so command.RetryMiddleware exercises cleanly.
	if db.failFirstPut.CompareAndSwap(true, false) {
		return errTransientWrite
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.orders[rec.ID] = rec
	return nil
}

func (db *relationalDB) cancelOrder(orderID string) error {
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

// selectOrderSummary mirrors a thin SQL JOIN tuned solely for presentation reads.
func (db *relationalDB) selectOrderSummary(orderID string) (OrderSummary, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	rec, ok := db.orders[orderID]
	if !ok {
		return OrderSummary{}, errOrderNotFound
	}
	customerName := db.customers[rec.CustomerID]
	return OrderSummary{
		OrderID:      rec.ID,
		CustomerName: customerName,
		Status:       rec.Status,
		DisplayTotal: fmt.Sprintf("$%.2f", float64(rec.TotalCents)/100),
	}, nil
}

func buildBuses(db *relationalDB, logger *slog.Logger) (cmdBus *command.Bus, qBus *query.Bus, err error) {
	cmdBus, err = command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: 5 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				RetryIf: func(err error) bool {
					return errors.Is(err, errTransientWrite)
				},
			}),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errAlreadyCanceled) || errors.Is(err, errInvalidOrderTotal) {
				return command.Reject(err)
			}
			return err
		}),
		command.WithOnRejected(func(ctx context.Context, rejected any, err error) {
			logger.WarnContext(ctx, "command rejected",
				"trace_id", cqrs.TraceIDFromContext(ctx),
				"command_type", fmt.Sprintf("%T", rejected),
				"err", err,
			)
		}),
	)
	if err != nil {
		return nil, nil, err
	}

	if regErr := command.Register(cmdBus, command.Func[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		if cmd.TotalCents <= 0 {
			return errInvalidOrderTotal
		}
		logger.InfoContext(ctx, "placing order",
			"order_id", cmd.OrderID,
			"trace_id", cqrs.TraceIDFromContext(ctx),
			"command_id", cqrs.CommandIDFromContext(ctx),
		)
		return db.insertOrder(orderRecord{
			ID:         cmd.OrderID,
			CustomerID: cmd.CustomerID,
			Status:     statusPending,
			TotalCents: cmd.TotalCents,
		})
	})); regErr != nil {
		return nil, nil, regErr
	}

	if regErr := command.Register(cmdBus, command.Func[CancelOrder](func(_ context.Context, cmd CancelOrder) error {
		return db.cancelOrder(cmd.OrderID)
	})); regErr != nil {
		return nil, nil, regErr
	}

	qBus, err = query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, nil, err
	}

	if regErr := query.Register(qBus, query.Func[GetOrderSummary, OrderSummary](
		func(_ context.Context, q GetOrderSummary) (OrderSummary, error) {
			return db.selectOrderSummary(q.OrderID)
		},
	)); regErr != nil {
		return nil, nil, regErr
	}

	return cmdBus, qBus, nil
}

func run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	db := newRelationalDB()

	cmdBus, qBus, err := buildBuses(db, logger)
	if err != nil {
		return err
	}

	reqCtx := cqrs.WithTraceID(ctx, "req-demo-1")

	if dispatchErr := cmdBus.Dispatch(reqCtx, PlaceOrder{
		OrderID:    "ord-100",
		CustomerID: "cust-42",
		TotalCents: 14900,
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err := query.Execute[OrderSummary](reqCtx, qBus, GetOrderSummary{OrderID: "ord-100"})
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after PlaceOrder:  %+v\n", summary)

	// Cancel once succeeds; canceling again is masked as a domain rejection via WithOnRejected.
	if dispatchErr := cmdBus.Dispatch(reqCtx, CancelOrder{OrderID: "ord-100", Reason: "customer request"}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, CancelOrder{OrderID: "ord-100", Reason: "duplicate click"}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err = query.Execute[OrderSummary](reqCtx, qBus, GetOrderSummary{OrderID: "ord-100"})
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after CancelOrder: %+v\n", summary)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
