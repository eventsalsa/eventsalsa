package main

import (
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/config"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/acl"
	infraqueries "eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/queries"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/cqrs/query"
)

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
	retryCfg config.RetryConfig,
	orders persistence.Adapter[domain.Order, any],
) (*command.Bus, error) {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: retryCfg.MaxAttempts,
				RetryIf:     isRetryableError,
			}),
		),
		command.WithErrorClassifier(classifyDomainError),
	)
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
