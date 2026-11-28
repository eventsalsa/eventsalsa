package bootstrap

import (
	"errors"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/config"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/acl"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
)

func classifyCommandError(err error) error {
	if domain.IsRejection(err) || errors.Is(err, acl.ErrEmptyCommand) {
		return command.Reject(err)
	}
	return err
}

// NewCommandBus builds the write-side command bus with observability, OCC retry middleware,
// boundary rejection classification, and registered Order handlers.
func NewCommandBus(
	retryCfg config.RetryConfig,
	orders persistence.Adapter[domain.Order, any],
	extraOpts ...command.BusOption,
) (*command.Bus, error) {
	opts := make([]command.BusOption, 0, 2+len(extraOpts))
	opts = append(opts,
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: retryCfg.MaxAttempts,
				RetryIf:     postgres.IsConcurrencyConflict,
			}),
		),
		command.WithErrorClassifier(classifyCommandError),
	)
	opts = append(opts, extraOpts...)

	bus, err := command.NewBus(opts...)
	if err != nil {
		return nil, err
	}
	if regErr := RegisterOrderCommands(bus, orders); regErr != nil {
		return nil, regErr
	}
	return bus, nil
}

// RegisterOrderCommands binds Order Decider handlers to ACL boundary translators on bus.
func RegisterOrderCommands(
	bus *command.Bus,
	orders persistence.Adapter[domain.Order, any],
) error {
	placeHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.PlaceOrder) string { return cmd.OrderID },
		domain.DecidePlaceOrder,
	)
	if err != nil {
		return err
	}
	if regErr := command.Register(bus, acl.NewPlaceOrderACL(placeHandler)); regErr != nil {
		return regErr
	}

	addItemHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.AddOrderItem) string { return cmd.OrderID },
		domain.DecideAddOrderItem,
	)
	if err != nil {
		return err
	}
	if regErr := command.Register(bus, acl.NewAddOrderItemACL(addItemHandler)); regErr != nil {
		return regErr
	}

	confirmHandler, err := persistence.NewCommandHandler(
		orders,
		func(cmd domain.ConfirmOrder) string { return cmd.OrderID },
		domain.DecideConfirmOrder,
	)
	if err != nil {
		return err
	}
	return command.Register(bus, acl.NewConfirmOrderACL(confirmHandler))
}
