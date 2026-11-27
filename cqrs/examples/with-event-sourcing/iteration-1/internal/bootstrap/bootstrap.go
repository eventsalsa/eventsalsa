// Package bootstrap wires the composition root for Event-Sourced Iteration 1:
// PostgreSQL pool & schema setup, eventsalsa/store, transaction/retry middlewares,
// application write handlers, and SQL read handlers.
package bootstrap

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/application"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/infra"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

// App holds the wired PostgreSQL pool and CQRS buses for Event-Sourced Iteration 1.
type App struct {
	Commands *command.Bus
	Queries  *query.Bus
	pool     *pgxpool.Pool
}

// Close releases the underlying database pool if initialized.
func (a *App) Close() {
	if a != nil && a.pool != nil {
		a.pool.Close()
	}
}

// Open connects to PostgreSQL, ensures the event store and projection schema,
// and wires the command and query buses.
func Open(ctx context.Context, dsn string) (*App, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if schemaErr := infra.EnsureSchema(ctx, pool); schemaErr != nil {
		pool.Close()
		return nil, schemaErr
	}

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	cmdBus, err := NewCommandBus(pool, es)
	if err != nil {
		pool.Close()
		return nil, err
	}
	qBus, err := NewQueryBus(pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &App{
		Commands: cmdBus,
		Queries:  qBus,
		pool:     pool,
	}, nil
}

// NewCommandBus configures observability, OCC retries, pgx transactions, and write handlers.
func NewCommandBus(pool *pgxpool.Pool, es *storepostgres.Store) (*command.Bus, error) {
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

// NewQueryBus configures read-side middleware and registers projected SQL query handlers.
func NewQueryBus(pool *pgxpool.Pool) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infra.GetOrderOverviewHandler{DB: pool}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
