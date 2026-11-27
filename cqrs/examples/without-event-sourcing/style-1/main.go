// Package main wires Style 1: top-level Go commands and queries, domain invariants
// inside internal/domain, write handlers inside internal/application, and DB/query
// handlers inside internal/infra.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/internal/infra"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-1/queries"
	"eventsalsa.dev/cqrs/query"
)

func newCommandBus(db *infra.MemoryDB) (*command.Bus, error) {
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

func run(ctx context.Context) error {
	db := infra.NewMemoryDB()

	cmdBus, err := newCommandBus(db)
	if err != nil {
		return err
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		return err
	}

	reqCtx := cqrs.WithTraceID(ctx, "req-style1-1")

	if dispatchErr := cmdBus.Dispatch(reqCtx, commands.PlaceOrder{
		OrderID:    "ord-100",
		CustomerID: "cust-42",
		TotalCents: 14900,
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err := query.Execute[queries.OrderSummary](reqCtx, qBus, queries.GetOrderSummary{OrderID: "ord-100"})
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after PlaceOrder:  %+v\n", summary)

	// Cancel once succeeds; canceling again is masked as a domain rejection via WithOnRejected.
	if dispatchErr := cmdBus.Dispatch(reqCtx, commands.CancelOrder{
		OrderID: "ord-100",
		Reason:  "customer request",
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, commands.CancelOrder{
		OrderID: "ord-100",
		Reason:  "duplicate click",
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err = query.Execute[queries.OrderSummary](reqCtx, qBus, queries.GetOrderSummary{OrderID: "ord-100"})
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after CancelOrder: %+v\n", summary)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
