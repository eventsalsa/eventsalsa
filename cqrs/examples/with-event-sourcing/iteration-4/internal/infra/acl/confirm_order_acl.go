package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
)

// NewConfirmOrderACL adapts public Protobuf ConfirmOrder messages into internal domain commands.
func NewConfirmOrderACL(next command.Handler[domain.ConfirmOrder]) command.Func[*commands.ConfirmOrder] {
	return func(ctx context.Context, cmd *commands.ConfirmOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, domain.ConfirmOrder{
			OrderID: cmd.GetOrderId(),
		})
	}
}
