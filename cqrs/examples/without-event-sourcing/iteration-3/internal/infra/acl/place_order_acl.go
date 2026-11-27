package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
)

// NewPlaceOrderACL adapts public Protobuf PlaceOrder messages into private domain commands
// and delegates execution to the Decider handler.
func NewPlaceOrderACL(next command.Handler[domain.PlaceOrder]) command.Func[*commands.PlaceOrder] {
	return func(ctx context.Context, cmd *commands.PlaceOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, domain.PlaceOrder{
			OrderID:    cmd.GetOrderId(),
			CustomerID: cmd.GetCustomerId(),
			TotalCents: cmd.GetTotalCents(),
		})
	}
}
