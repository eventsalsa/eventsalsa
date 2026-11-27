// Package main demonstrates CQRS combined with Event Sourcing over eventsalsa/store.
//
// Write commands execute inside a PostgreSQL transaction middleware, fold the
// Order stream via pure initial/evolve/decide functions, append events to
// eventsalsa.dev/store with optimistic concurrency + causal metadata, and update an
// inline read-model table. Queries read directly from order_overview_v1.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

func newCommandBus(pool *pgxpool.Pool, es *storepostgres.Store) (*command.Bus, error) {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf: func(err error) bool {
					return errors.Is(err, store.ErrOptimisticConcurrency)
				},
			}),
			withPgxTx(pool),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errAlreadyPlaced) || errors.Is(err, errNotPlaced) || errors.Is(err, errInvalidAmount) {
				return command.Reject(err)
			}
			return err
		}),
	)
	if err != nil {
		return nil, err
	}

	if regErr := command.Register(bus, &PlaceOrderHandler{store: es}); regErr != nil {
		return nil, regErr
	}
	if regErr := command.Register(bus, &ConfirmOrderHandler{store: es}); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(pool *pgxpool.Pool) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &GetOrderOverviewHandler{db: pool}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local development example default DSN
		dsn = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // G101: default local DSN
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if schemaErr := ensureSchema(ctx, pool); schemaErr != nil {
		return schemaErr
	}

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	cmdBus, err := newCommandBus(pool, es)
	if err != nil {
		return err
	}
	qBus, err := newQueryBus(pool)
	if err != nil {
		return err
	}

	orderID := "ord-" + uuid.NewString()[:8]
	reqCtx := cqrs.WithTraceID(ctx, "trace-"+uuid.NewString()[:8])

	if dispatchErr := cmdBus.Dispatch(reqCtx, PlaceOrder{
		OrderID:    orderID,
		CustomerID: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, ConfirmOrder{OrderID: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[OrderOverview](reqCtx, qBus, GetOrderOverview{OrderID: orderID})
	if err != nil {
		return err
	}
	fmt.Printf("Projected overview: %+v\n", view)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
