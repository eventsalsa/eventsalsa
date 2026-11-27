package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-2/internal/application"
)

// NewCancelOrderACL adapts public Protobuf CancelOrder messages into internal application
// use-case commands and delegates execution to the write handler.
func NewCancelOrderACL(next command.Handler[application.CancelOrder]) command.Func[*commands.CancelOrder] {
	return func(ctx context.Context, cmd *commands.CancelOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, application.CancelOrder{
			OrderID: cmd.GetOrderId(),
			Reason:  cmd.GetReason(),
		})
	}
}
