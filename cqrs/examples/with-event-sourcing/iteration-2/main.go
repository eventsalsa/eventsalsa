// Package main wires Iteration 2 of Event-Sourced CQRS:
//
// Top-level commands/ and queries/ export compiled Protobuf contracts; internal/infra/acl
// translates external Protobuf commands via command.Func adapters into internal/application
// use-case commands; internal/domain owns the event-sourced Order aggregate and its
// OrderRepository port over internal/domain/events; and internal/infra/persistence +
// internal/infra/queries run commands inside PostgreSQL transactions over eventsalsa/store,
// eventmap-gen mappings, OrderOverviewProjection, and thin Protobuf read handlers.
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
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/application"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/queries"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

func classifyDomainError(err error) error {
	if errors.Is(err, domain.ErrAlreadyPlaced) ||
		errors.Is(err, domain.ErrOrderNotFound) ||
		errors.Is(err, domain.ErrNotPlaced) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

func registerCommandHandlers(bus *command.Bus, orders domain.OrderRepository) error {
	placeOrderApp := &application.PlaceOrderHandler{Orders: orders}
	if err := command.Register(bus, acl.NewPlaceOrderACL(placeOrderApp)); err != nil {
		return err
	}
	confirmOrderApp := &application.ConfirmOrderHandler{Orders: orders}
	return command.Register(bus, acl.NewConfirmOrderACL(confirmOrderApp))
}

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
			persistence.WithPgxTx(pool),
		),
		command.WithErrorClassifier(classifyDomainError),
	)
	if err != nil {
		return nil, err
	}

	orders := &persistence.OrderRepository{
		Store:      es,
		Projection: &persistence.OrderOverviewProjection{},
	}
	if regErr := registerCommandHandlers(bus, orders); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(pool *pgxpool.Pool) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infraqueries.GetOrderOverviewHandler{DB: pool}); regErr != nil {
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

	if schemaErr := persistence.EnsureSchema(ctx, pool); schemaErr != nil {
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

	if dispatchErr := cmdBus.Dispatch(reqCtx, &commands.PlaceOrder{
		OrderId:    orderID,
		CustomerId: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, &commands.ConfirmOrder{OrderId: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[*queries.OrderOverview](
		reqCtx,
		qBus,
		&queries.GetOrderOverview{OrderId: orderID},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Projected overview: order_id=%s customer_id=%s status=%s total_cents=%d version=%d\n",
		view.GetOrderId(), view.GetCustomerId(), view.GetStatus(), view.GetTotalCents(), view.GetVersion())
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
