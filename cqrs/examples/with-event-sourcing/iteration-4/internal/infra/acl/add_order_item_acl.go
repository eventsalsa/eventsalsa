package acl

import (
	"context"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/commands"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
)

// NewAddOrderItemACL adapts public Protobuf AddOrderItem messages into internal domain commands.
func NewAddOrderItemACL(next command.Handler[domain.AddOrderItem]) command.Func[*commands.AddOrderItem] {
	return func(ctx context.Context, cmd *commands.AddOrderItem) error {
		if cmd == nil {
			return ErrEmptyCommand
		}
		return next.Handle(ctx, domain.AddOrderItem{
			OrderID:   cmd.GetOrderId(),
			SKU:       cmd.GetSku(),
			UnitCents: cmd.GetUnitCents(),
			Quantity:  cmd.GetQuantity(),
		})
	}
}
