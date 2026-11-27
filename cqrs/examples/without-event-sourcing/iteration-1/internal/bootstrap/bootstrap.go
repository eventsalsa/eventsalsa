// Package bootstrap wires the composition root for Iteration 1 (stores, middlewares,
// error classification, command handlers, and query handlers).
package bootstrap

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

// App holds the wired command and query buses for Iteration 1.
type App struct {
	Commands *command.Bus
	Queries  *query.Bus
}

// New constructs a fully wired in-memory App instance.
func New() (*App, error) {
	db := infra.NewMemoryDB()
	cmdBus, err := NewCommandBus(db)
	if err != nil {
		return nil, err
	}
	qBus, err := NewQueryBus(db)
	if err != nil {
		return nil, err
	}
	return &App{
		Commands: cmdBus,
		Queries:  qBus,
	}, nil
}

// NewCommandBus configures middleware, rejection classification, and application write handlers.
func NewCommandBus(db *infra.MemoryDB) (*command.Bus, error) {
	bus, err := command.NewBus(
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

// NewQueryBus configures read-side middleware and registers SQL-like query handlers.
func NewQueryBus(db *infra.MemoryDB) (*query.Bus, error) {
	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return nil, err
	}
	if regErr := query.Register(qBus, &infra.GetOrderSummaryHandler{DB: db}); regErr != nil {
		return nil, regErr
	}
	return qBus, nil
}
