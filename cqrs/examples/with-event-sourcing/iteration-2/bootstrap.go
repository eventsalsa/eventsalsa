package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/application"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/infra/queries"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

type wiredApp struct {
	commands *command.Bus
	queries  *query.Bus
	pool     *pgxpool.Pool
}

func (a *wiredApp) close() {
	if a != nil && a.pool != nil {
		a.pool.Close()
	}
}

func openApp(ctx context.Context, dsn string) (*wiredApp, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if schemaErr := persistence.EnsureSchema(ctx, pool); schemaErr != nil {
		pool.Close()
		return nil, schemaErr
	}

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	cmdBus, err := newCommandBus(pool, es)
	if err != nil {
		pool.Close()
		return nil, err
	}
	qBus, err := newQueryBus(pool)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &wiredApp{
		commands: cmdBus,
		queries:  qBus,
		pool:     pool,
	}, nil
}

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
