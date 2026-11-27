// Package main runs Iteration 1: top-level Go commands and queries dispatched over the
// buses assembled by internal/bootstrap.
package main

import (
	"context"
	"fmt"
	"os"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/bootstrap"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/queries"
	"eventsalsa.dev/cqrs/query"
)

func run(ctx context.Context) error {
	app, err := bootstrap.New()
	if err != nil {
		return err
	}

	reqCtx := cqrs.WithTraceID(ctx, "req-iteration1-1")

	if dispatchErr := app.Commands.Dispatch(reqCtx, commands.PlaceOrder{
		OrderID:    "ord-100",
		CustomerID: "cust-42",
		TotalCents: 14900,
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err := query.Execute[queries.OrderSummary](reqCtx, app.Queries, queries.GetOrderSummary{OrderID: "ord-100"})
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after PlaceOrder:  %+v\n", summary)

	// Cancel once succeeds; canceling again is masked as a domain rejection via WithOnRejected.
	if dispatchErr := app.Commands.Dispatch(reqCtx, commands.CancelOrder{
		OrderID: "ord-100",
		Reason:  "customer request",
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := app.Commands.Dispatch(reqCtx, commands.CancelOrder{
		OrderID: "ord-100",
		Reason:  "duplicate click",
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err = query.Execute[queries.OrderSummary](reqCtx, app.Queries, queries.GetOrderSummary{OrderID: "ord-100"})
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
