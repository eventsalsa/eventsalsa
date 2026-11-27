package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/acl"
	infrapersistence "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/snapshot"
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

func openApp(ctx context.Context, dsn string) (*wiredApp, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	if schemaErr := infrapersistence.EnsureSchema(ctx, pool); schemaErr != nil {
		pool.Close()
		return nil, schemaErr
	}

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	snapStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())
	cmdBus, err := newCommandBus(pool, es, snapStore)
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

func orderDecider() persistence.Decider[domain.Order, any] {
	return persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
}

func isRetryableError(err error) bool {
	return postgres.IsConcurrencyConflict(err)
}

func classifyDomainError(err error) error {
	if errors.Is(err, domain.ErrAlreadyPlaced) ||
		errors.Is(err, domain.ErrAlreadyConfirmed) ||
		errors.Is(err, domain.ErrNotPlaced) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, domain.ErrEmptySKU) ||
		errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

func registerCommandHandlers(
	bus *command.Bus,
	orders persistence.Adapter[domain.Order, any],
) error {
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

	addItemHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.AddOrderItem) string { return cmd.OrderID },
		domain.DecideAddOrderItem,
	)
	if err != nil {
		return err
	}
	if regErr := command.Register(bus, acl.NewAddOrderItemACL(addItemHandler)); regErr != nil {
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

func newCommandBus(
	pool *pgxpool.Pool,
	es *storepostgres.Store,
	snapStore *snapshotpostgres.Store,
) (*command.Bus, error) {
	// Narrow transactions by default: NewSnapshotAdapter loads snapshots + delta events in InReadTx,
	// releases the DB connection before decide runs, then runs Append + inline projections +
	// savepoint-isolated conditional snapshot writes inside InTx.
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf:     isRetryableError,
			}),
		),
		command.WithErrorClassifier(classifyDomainError),
	)
	if err != nil {
		return nil, err
	}

	codec, err := postgres.NewEventMapCodec(
		infrapersistence.MarshalEvent,
		infrapersistence.FromESEventAs[any],
	)
	if err != nil {
		return nil, err
	}
	orders, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[domain.Order, any]{
		Decider: orderDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "Order",
			Codec:       codec,
			Projections: []postgres.Projection{&infrapersistence.OrderOverviewProjection{}},
		},
		Snapshot: postgres.SnapshotConfig[domain.Order]{
			Store:         snapStore,
			Codec:         snapshot.JSON[domain.Order](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(3),
			Version:       func(o domain.Order) int64 { return o.Version },
			Transformers:  []snapshot.PayloadTransformer{snapshot.Gzip()},
		},
	})
	if err != nil {
		return nil, err
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
