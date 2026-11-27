// Package main demonstrates Iteration 3: top-level Protobuf contracts and Decider/StateRepository
// wiring (via eventsalsa.dev/cqrs/persistence) assembled in bootstrap.go.
package main

import (
	"context"
	"fmt"
	"os"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/queries"
	"eventsalsa.dev/cqrs/query"
)

func run(ctx context.Context) error {
	app, err := newApp()
	if err != nil {
		return err
	}

	reqCtx := cqrs.WithTraceID(ctx, "req-iteration3-1")

	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.PlaceOrder{
		OrderId:    "ord-100",
		CustomerId: "cust-42",
		TotalCents: 14900,
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err := query.Execute[*queries.OrderSummary](
		reqCtx,
		app.queries,
		&queries.GetOrderSummary{OrderId: "ord-100"},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Read DTO after PlaceOrder:  order_id=%s customer=%s status=%s total=%s\n",
		summary.GetOrderId(), summary.GetCustomerName(), summary.GetStatus(), summary.GetDisplayTotal())

	// Cancel once succeeds; canceling again is masked as a domain rejection via WithOnRejected.
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.CancelOrder{
		OrderId: "ord-100",
		Reason:  "customer request",
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.CancelOrder{
		OrderId: "ord-100",
		Reason:  "duplicate click",
	}); dispatchErr != nil {
		return dispatchErr
	}

	summary, err = query.Execute[*queries.OrderSummary](
		reqCtx,
		app.queries,
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
