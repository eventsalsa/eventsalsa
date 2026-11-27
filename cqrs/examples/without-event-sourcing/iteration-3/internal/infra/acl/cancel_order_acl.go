package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/commands"
	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
)

// NewCancelOrderACL adapts public Protobuf CancelOrder messages into private domain commands
// and delegates execution to the Decider handler.
func NewCancelOrderACL(next command.Handler[domain.CancelOrder]) command.Func[*commands.CancelOrder] {
	return func(ctx context.Context, cmd *commands.CancelOrder) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, domain.CancelOrder{
			OrderID: cmd.GetOrderId(),
			Reason:  cmd.GetReason(),
		})
	}
}
