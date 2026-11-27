// Package main wires Iteration 3: top-level Protobuf contracts, command.Func ACL
// translators inside internal/infra/acl, a single generic Decider handler in
// internal/application, pure Initial/Decide/Evolve domain logic in internal/domain,
// and state persistence/read handlers inside internal/infra.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/application"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/acl"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/persistence"
	infraqueries "eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/infra/queries"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
	"eventsalsa.dev/cqrs/query"
)

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
			if errors.Is(err, domain.ErrAlreadyPlaced) ||
				errors.Is(err, domain.ErrNotPlaced) ||
				errors.Is(err, domain.ErrAlreadyCanceled) ||
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

	placeApp := application.NewDeciderHandler(
		db,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	)
	if regErr := command.Register(bus, acl.NewPlaceOrderACL(placeApp)); regErr != nil {
		return nil, regErr
	}

	cancelApp := application.NewDeciderHandler(
		db,
		func(cmd domain.CancelOrder) string { return cmd.OrderID },
		domain.DecideCancelOrder,
	)
	if regErr := command.Register(bus, acl.NewCancelOrderACL(cancelApp)); regErr != nil {
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

func run(ctx context.Context) error {
	db := persistence.NewMemoryDB()

	cmdBus, err := newCommandBus(db)
	if err != nil {
		return err
	}
	qBus, err := newQueryBus(db)
	if err != nil {
		return err
	}

	reqCtx := cqrs.WithTraceID(ctx, "req-iteration3-1")

	if dispatchErr := cmdBus.Dispatch(reqCtx, &commands.PlaceOrder{
		OrderId:    "ord-100",
		CustomerId: "cust-42",
		TotalCents: 14900,
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err := query.Execute[*queries.OrderSummary](
		reqCtx,
		qBus,
		&queries.GetOrderSummary{OrderId: "ord-100"},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after PlaceOrder:  order_id=%s customer=%s status=%s total=%s\n",
		summary.GetOrderId(), summary.GetCustomerName(), summary.GetStatus(), summary.GetDisplayTotal())

	// Cancel once succeeds; canceling again is masked as a domain rejection via WithOnRejected.
	if dispatchErr := cmdBus.Dispatch(reqCtx, &commands.CancelOrder{
		OrderId: "ord-100",
		Reason:  "customer request",
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, &commands.CancelOrder{
		OrderId: "ord-100",
		Reason:  "duplicate click",
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err = query.Execute[*queries.OrderSummary](
		reqCtx,
		qBus,
		&queries.GetOrderSummary{OrderId: "ord-100"},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after CancelOrder: order_id=%s customer=%s status=%s total=%s\n",
		summary.GetOrderId(), summary.GetCustomerName(), summary.GetStatus(), summary.GetDisplayTotal())
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
