// Package main demonstrates CQRS combined with Event Sourcing over eventsalsa/store.
//
// Top-level commands/ and queries/ export plain Go contracts; internal/domain holds
// the Order aggregate (PlaceOrder, Confirm, ReconstituteOrder, Evolve) and its OrderRepository
// port over internal/domain/events; internal/application orchestrates writes through
// domain.OrderRepository; and internal/infra executes commands inside a PostgreSQL transaction
// middleware, maps events via eventmap-gen, appends them with optimistic concurrency + causal
// metadata, and runs OrderOverviewProjection inline into order_overview_v1.
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
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/application"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/infra"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/queries"
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
			infra.WithPgxTx(pool),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, domain.ErrAlreadyPlaced) ||
				errors.Is(err, domain.ErrOrderNotFound) ||
				errors.Is(err, domain.ErrNotPlaced) ||
				errors.Is(err, domain.ErrInvalidAmount) {
				return command.Reject(err)
			}
			return err
		}),
	)
	if err != nil {
		return nil, err
	}

	orders := &infra.OrderRepository{
		Store:      es,
		Projection: &infra.OrderOverviewProjection{},
	}
	if regErr := command.Register(bus, &application.PlaceOrderHandler{Orders: orders}); regErr != nil {
		return nil, regErr
	}
	if regErr := command.Register(bus, &application.ConfirmOrderHandler{Orders: orders}); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(pool *pgxpool.Pool) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infra.GetOrderOverviewHandler{DB: pool}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local development example default DSN
		dsn = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // local DSN
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if schemaErr := infra.EnsureSchema(ctx, pool); schemaErr != nil {
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

	if dispatchErr := cmdBus.Dispatch(reqCtx, commands.PlaceOrder{
		OrderID:    orderID,
		CustomerID: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, commands.ConfirmOrder{OrderID: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[queries.OrderOverview](reqCtx, qBus, queries.GetOrderOverview{OrderID: orderID})
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
