package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/infra/queries"
	"eventsalsa.dev/cqrs/query"
)

type wiredApp struct {
	commands *command.Bus
	queries  *query.Bus
}

func newApp() (*wiredApp, error) {
	db := persistence.NewMemoryDB()
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

func newCommandBus(db *persistence.MemoryDB) (*command.Bus, error) {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: 5 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				RetryIf: func(err error) bool {
					return errors.Is(err, persistence.ErrTransientWrite)
				},
			}),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, domain.ErrAlreadyCanceled) ||
				errors.Is(err, domain.ErrInvalidOrderTotal) ||
				errors.Is(err, domain.ErrCancellationReasonRequired) ||
				errors.Is(err, acl.ErrEmptyCommand) {
				return command.Reject(err)
			}
			return err
		}),
		command.WithOnRejected(func(ctx context.Context, rejected any, err error) {
			fmt.Printf("Command rejected: trace_id=%s command_type=%T err=%v\n",
				cqrs.TraceIDFromContext(ctx), rejected, err)
		}),
	)
	if err != nil {
		return nil, err
	}

	placeOrderApp := &application.PlaceOrderHandler{
		Orders: db,
	}
	if regErr := command.Register(bus, acl.NewPlaceOrderACL(placeOrderApp)); regErr != nil {
		return nil, regErr
	}

	cancelOrderApp := &application.CancelOrderHandler{
		Orders: db,
	}
	if regErr := command.Register(bus, acl.NewCancelOrderACL(cancelOrderApp)); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(db *persistence.MemoryDB) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infraqueries.GetOrderSummaryHandler{DB: db}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
