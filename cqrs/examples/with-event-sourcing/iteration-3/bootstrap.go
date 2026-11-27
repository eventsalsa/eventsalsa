package main

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

func isRetryableError(err error) bool {
	return errors.Is(err, persistence.ErrConcurrencyConflict) ||
		errors.Is(err, store.ErrOptimisticConcurrency)
}

func classifyDomainError(err error) error {
	if errors.Is(err, domain.ErrAlreadyPlaced) ||
		errors.Is(err, domain.ErrNotPlaced) ||
		errors.Is(err, domain.ErrInvalidAmount) ||
		errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

func registerCommandHandlers(bus *command.Bus, repo persistence.EventRepository[any]) error {
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

func newCommandBus(pool *pgxpool.Pool, es *storepostgres.Store) (*command.Bus, error) {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf:     isRetryableError,
			}),
			infrapersistence.WithPgxTx(pool),
		),
		command.WithErrorClassifier(classifyDomainError),
	)
	if err != nil {
		return nil, err
	}

	orders := &infrapersistence.OrderRepository{
		Store:      es,
		Projection: &infrapersistence.OrderOverviewProjection{},
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
