// Package bootstrap wires the composition root for Event-Sourced Iteration 3:
// PostgreSQL pool & schema setup, eventsalsa/store, eventsalsa.dev/cqrs/persistence
// (FromEventRepository + NewCommandHandler), ACL translators, middlewares, error
// classification, and command/query buses.
package bootstrap

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/acl"
	infrapersistence "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

// App holds the wired PostgreSQL pool and CQRS buses for Event-Sourced Iteration 3.
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

	if schemaErr := infrapersistence.EnsureSchema(ctx, pool); schemaErr != nil {
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

// ClassifyDomainError marks domain and ACL validation errors as business rejections.
func ClassifyDomainError(err error) error {
	if errors.Is(err, domain.ErrAlreadyPlaced) ||
		errors.Is(err, domain.ErrNotPlaced) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

// RegisterCommandHandlers binds repo and the Order Decider via FromEventRepository and
// registers ACL-wrapped command handlers on bus.
func RegisterCommandHandlers(bus *command.Bus, repo persistence.EventRepository[any]) error {
	decider := persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
	orders := persistence.FromEventRepository(repo, decider)

	placeHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	)
	if err != nil {
		return err
	}
	if regErr := command.Register(bus, acl.NewPlaceOrderACL(placeHandler)); regErr != nil {
		return regErr
	}

	confirmHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.ConfirmOrder) string { return cmd.OrderID },
		domain.DecideConfirmOrder,
	)
	if err != nil {
		return err
	}
	return command.Register(bus, acl.NewConfirmOrderACL(confirmHandler))
}

// NewCommandBus configures observability, OCC retries, pgx transactions, and write handlers.
func NewCommandBus(pool *pgxpool.Pool, es *storepostgres.Store) (*command.Bus, error) {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf: func(err error) bool {
					return errors.Is(err, persistence.ErrConcurrencyConflict) ||
						errors.Is(err, store.ErrOptimisticConcurrency)
				},
			}),
			infrapersistence.WithPgxTx(pool),
		),
		command.WithErrorClassifier(ClassifyDomainError),
	)
	if err != nil {
		return nil, err
	}

	orders := &infrapersistence.OrderRepository{
		Store:      es,
		Projection: &infrapersistence.OrderOverviewProjection{},
	}
	if regErr := RegisterCommandHandlers(bus, orders); regErr != nil {
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
	if regErr := query.Register(qBus, &infraqueries.GetOrderOverviewHandler{DB: pool}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
