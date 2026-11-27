package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/acl"
	infrapersistence "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/query"
)

type wiredApp struct {
	commands *command.Bus
	queries  *query.Bus
}

func newApp() (*wiredApp, error) {
	db := infrapersistence.NewMemoryDB()
	cmdBus, err := newCommandBus(db)
	if err != nil {
		return nil, err
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		return nil, err
	}
	return &wiredApp{
		commands: cmdBus,
		queries:  qBus,
	}, nil
}

func isRetryableError(err error) bool {
	return errors.Is(err, infrapersistence.ErrTransientWrite) ||
		errors.Is(err, persistence.ErrConcurrencyConflict)
}

func classifyDomainError(err error) error {
	if errors.Is(err, domain.ErrAlreadyPlaced) ||
		errors.Is(err, domain.ErrNotPlaced) ||
		errors.Is(err, domain.ErrAlreadyCanceled) ||
		errors.Is(err, domain.ErrInvalidOrderTotal) ||
		errors.Is(err, domain.ErrCancellationReasonRequired) ||
		errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

func newCommandBus(db *infrapersistence.MemoryDB, extraOpts ...command.BusOption) (*command.Bus, error) {
	opts := make([]command.BusOption, 0, 3+len(extraOpts))
	opts = append(opts,
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: 5 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				RetryIf:     isRetryableError,
			}),
		),
		command.WithErrorClassifier(classifyDomainError),
		command.WithOnRejected(func(ctx context.Context, rejected any, err error) {
			fmt.Printf("Command rejected: trace_id=%s command_type=%T err=%v\n",
				cqrs.TraceIDFromContext(ctx), rejected, err)
		}),
	)
	opts = append(opts, extraOpts...)

	bus, err := command.NewBus(opts...)
	if err != nil {
		return nil, err
	}

	decider := persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
	orders := persistence.FromStateRepository(db, decider)

	placeHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	)
	if err != nil {
		return nil, err
	}
	if regErr := command.Register(bus, acl.NewPlaceOrderACL(placeHandler)); regErr != nil {
		return nil, regErr
	}

	cancelHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.CancelOrder) string { return cmd.OrderID },
		domain.DecideCancelOrder,
	)
	if err != nil {
		return nil, err
	}
	if regErr := command.Register(bus, acl.NewCancelOrderACL(cancelHandler)); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(db *infrapersistence.MemoryDB) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infraqueries.GetOrderSummaryHandler{DB: db}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
