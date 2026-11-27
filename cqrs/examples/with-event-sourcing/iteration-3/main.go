// Package main runs Event-Sourced Iteration 3 using top-level Protobuf contracts over
// the PostgreSQL buses assembled by internal/bootstrap.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/bootstrap"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/queries"
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

	if dispatchErr := app.Commands.Dispatch(reqCtx, &commands.PlaceOrder{
		OrderId:    orderID,
		CustomerId: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := app.Commands.Dispatch(reqCtx, &commands.ConfirmOrder{OrderId: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[*queries.OrderOverview](
		reqCtx,
		app.Queries,
		&queries.GetOrderOverview{OrderId: orderID},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Projected overview: order_id=%s customer_id=%s status=%s total_cents=%d version=%d\n",
		view.GetOrderId(), view.GetCustomerId(), view.GetStatus(), view.GetTotalCents(), view.GetVersion())
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
