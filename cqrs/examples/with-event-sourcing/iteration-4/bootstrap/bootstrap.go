// Package bootstrap composes the runtime application container, buses, and persistence wiring
// for Event-Sourced Iteration 4.
package bootstrap

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/config"
	infrapersistence "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/snapshot"
	snapshotpostgres "eventsalsa.dev/snapshot/postgres"
	storepostgres "eventsalsa.dev/store/postgres"
)

// App holds initialized runtime buses and the underlying PostgreSQL connection pool.
type App struct {
	Commands *command.Bus
	Queries  *query.Bus
	pool     *pgxpool.Pool
}

// Close releases PostgreSQL connections held by App.
func (a *App) Close() {
	if a != nil && a.pool != nil {
		a.pool.Close()
	}
}

// Open validates cfg, initializes PostgreSQL pools/schemas/stores, and wires command and query buses.
func Open(ctx context.Context, cfg config.Config) (*App, error) {
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

	orders, err := infrapersistence.NewOrderSnapshotAdapter(
		pool,
		storepostgres.NewStore(storepostgres.DefaultStoreConfig()),
		snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig()),
		infrapersistence.WithStreamType(cfg.Snapshot.StreamType),
		infrapersistence.WithSchemaVersion(cfg.Snapshot.SchemaVersion),
		infrapersistence.WithSnapshotPolicy(snapshot.EveryNEvents(cfg.Snapshot.EveryNEvents)),
		infrapersistence.WithSnapshotCompression(cfg.Snapshot.Compress),
	)
	if err != nil {
		pool.Close()
		return nil, err
	}

	cmdBus, err := NewCommandBus(cfg.Retry, orders)
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
