package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/application"
)

// NewPlaceOrderACL adapts public Protobuf PlaceOrder messages into internal application
// use-case commands and delegates execution to the write handler.
func NewPlaceOrderACL(next command.Handler[application.PlaceOrder]) command.Func[*commands.PlaceOrder] {
	return func(ctx context.Context, cmd *commands.PlaceOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, application.PlaceOrder{
			OrderID:    cmd.GetOrderId(),
			CustomerID: cmd.GetCustomerId(),
			TotalCents: cmd.GetTotalCents(),
		})
	}
}
