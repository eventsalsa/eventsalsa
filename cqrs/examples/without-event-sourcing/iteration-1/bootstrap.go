package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/infra"
	"eventsalsa.dev/cqrs/query"
)

type wiredApp struct {
	commands *command.Bus
	queries  *query.Bus
}

func newApp() (*wiredApp, error) {
	db := infra.NewMemoryDB()
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

func newCommandBus(db *infra.MemoryDB, extraOpts ...command.BusOption) (*command.Bus, error) {
	opts := make([]command.BusOption, 0, 3+len(extraOpts))
	opts = append(opts,
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: 5 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				RetryIf: func(err error) bool {
					return errors.Is(err, infra.ErrTransientWrite)
				},
			}),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, domain.ErrAlreadyCanceled) || errors.Is(err, domain.ErrInvalidOrderTotal) {
				return command.Reject(err)
			}
			return err
		}),
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

	if regErr := command.Register(bus, &application.PlaceOrderHandler{
		Orders: db,
	}); regErr != nil {
		return nil, regErr
	}
	if regErr := command.Register(bus, &application.CancelOrderHandler{
		Orders: db,
	}); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

func newQueryBus(db *infra.MemoryDB) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infra.GetOrderSummaryHandler{DB: db}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
