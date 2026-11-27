// Package main runs Event-Sourced Iteration 1 using top-level Go contracts over
// the PostgreSQL buses assembled by internal/bootstrap.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/bootstrap"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/queries"
	"eventsalsa.dev/cqrs/query"
)

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local development example default DSN
		dsn = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // local DSN
	}

	app, err := bootstrap.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer app.Close()

	orderID := "ord-" + uuid.NewString()[:8]
	reqCtx := cqrs.WithTraceID(ctx, "trace-"+uuid.NewString()[:8])

	if dispatchErr := app.Commands.Dispatch(reqCtx, commands.PlaceOrder{
		OrderID:    orderID,
		CustomerID: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := app.Commands.Dispatch(reqCtx, commands.ConfirmOrder{OrderID: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[queries.OrderOverview](reqCtx, app.Queries, queries.GetOrderOverview{OrderID: orderID})
	if err != nil {
		return err
	}
	fmt.Printf("Projected overview: %+v\n", view)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
