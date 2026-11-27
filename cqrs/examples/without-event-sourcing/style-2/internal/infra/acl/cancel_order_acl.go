package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

// NewCancelOrderACL adapts public Protobuf CancelOrder messages into private domain commands
// and delegates execution to the application write handler.
func NewCancelOrderACL(next command.Handler[domain.CancelOrder]) command.Func[*commands.CancelOrder] {
	return func(ctx context.Context, cmd *commands.CancelOrder) error {
		if cmd == nil {
			return domain.ErrEmptyCommand
		}
		return next.Handle(ctx, domain.CancelOrder{
			OrderID: cmd.GetOrderId(),
			Reason:  cmd.GetReason(),
		})
	}
}
