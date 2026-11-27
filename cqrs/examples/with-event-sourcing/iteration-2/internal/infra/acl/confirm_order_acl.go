package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-2/internal/application"
)

// NewConfirmOrderACL adapts public Protobuf ConfirmOrder messages into internal application
// use-case commands and delegates execution to the write handler.
func NewConfirmOrderACL(next command.Handler[application.ConfirmOrder]) command.Func[*commands.ConfirmOrder] {
	return func(ctx context.Context, cmd *commands.ConfirmOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, application.ConfirmOrder{
			OrderID: cmd.GetOrderId(),
		})
	}
}
