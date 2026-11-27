// Package main runs Event-Sourced Iteration 4: top-level Protobuf contracts and
// snapshotted Decider wiring (eventsalsa.dev/cqrs/persistence + eventsalsa.dev/snapshot).
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/queries"
	"eventsalsa.dev/cqrs/query"
)

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local development example default DSN
		dsn = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // local DSN
	}

	app, err := openApp(ctx, dsn)
	if err != nil {
		return err
	}
	defer app.close()

	orderID := "ord-" + uuid.NewString()[:8]
	reqCtx := cqrs.WithTraceID(ctx, "trace-"+uuid.NewString()[:8])

	// v1: OrderPlaced
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.PlaceOrder{
		OrderId:    orderID,
		CustomerId: "cust-42",
		TotalCents: 10000,
	}); dispatchErr != nil {
		return dispatchErr
	}
	// v2: OrderItemAdded
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.AddOrderItem{
		OrderId:   orderID,
		Sku:       "sku-salsa",
		UnitCents: 2500,
		Quantity:  2,
	}); dispatchErr != nil {
		return dispatchErr
	}
	// v3: OrderItemAdded -> crosses EveryNEvents(3) and writes a compressed snapshot at v3.
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.AddOrderItem{
		OrderId:   orderID,
		Sku:       "sku-chips",
		UnitCents: 1900,
		Quantity:  1,
	}); dispatchErr != nil {
		return dispatchErr
	}
	// v4: ConfirmOrder loads snapshot v3 (zero events replayed) and appends OrderConfirmed.
	if dispatchErr := app.commands.Dispatch(reqCtx, &commands.ConfirmOrder{OrderId: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[*queries.OrderOverview](
		reqCtx,
		app.queries,
		&queries.GetOrderOverview{OrderId: orderID},
	)
	if err != nil {
		return err
	}
	fmt.Printf(
		"Projected overview: order_id=%s customer_id=%s status=%s item_count=%d total_cents=%d version=%d\n",
		view.GetOrderId(),
		view.GetCustomerId(),
		view.GetStatus(),
		view.GetItemCount(),
		view.GetTotalCents(),
		view.GetVersion(),
	)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
