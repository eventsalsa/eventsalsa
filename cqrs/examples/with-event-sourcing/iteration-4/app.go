package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/config"
	infrapersistence "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
	"eventsalsa.dev/cqrs/query"
	snapshotpostgres "eventsalsa.dev/snapshot/postgres"
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

func openApp(ctx context.Context, cfg config.Config) (*wiredApp, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if schemaErr := infrapersistence.EnsureSchema(ctx, pool); schemaErr != nil {
		pool.Close()
		return nil, schemaErr
	}

	orders, err := infrapersistence.NewOrderAdapter(&infrapersistence.OrderAdapterParams{
		DB:         pool,
		EventStore: storepostgres.NewStore(storepostgres.DefaultStoreConfig()),
		SnapStore:  snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig()),
		Snapshot:   cfg.Snapshot,
	})
	if err != nil {
		pool.Close()
		return nil, err
	}

	cmdBus, err := newCommandBus(cfg.Retry, orders)
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
