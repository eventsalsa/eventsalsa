package main

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

	if schemaErr := infra.EnsureSchema(ctx, pool); schemaErr != nil {
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
