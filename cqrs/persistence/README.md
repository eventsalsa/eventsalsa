# eventsalsa/cqrs/persistence

Database-agnostic `Decider`, `StateRepository`, `EventRepository`, and generic `command.Handler` runners for `eventsalsa.dev/cqrs`.

Depends strictly on `eventsalsa.dev/cqrs` and Go's standard library (zero database or driver imports). PostgreSQL-specific `eventsalsa/store`, `eventsalsa/snapshot`, and `pgx` adapters live in a dedicated downstream submodule.

## Usage

### With State Persistence (`StateRepository[S]`)

```go
orders, err := persistence.NewStateRunner(repo, persistence.Decider[domain.Order, any]{
	Initial: domain.InitialOrder,
	Evolve:  domain.EvolveOrder,
})
if err != nil {
	return err
}

placeHandler, err := persistence.NewCommandHandler(
	orders,
	func(cmd domain.PlaceOrder) string { return cmd.OrderID },
	domain.DecidePlaceOrder,
)
```

### With Event-Sourced Persistence (`EventRepository[E]`)

```go
orders, err := persistence.NewEventRunner(eventRepo, persistence.Decider[domain.Order, any]{
	Initial: domain.InitialOrder,
	Evolve:  domain.EvolveOrder,
})
if err != nil {
	return err
}

confirmHandler, err := persistence.NewCommandHandler(
	orders,
	func(cmd domain.ConfirmOrder) string { return cmd.OrderID },
	domain.DecideConfirmOrder,
)
```

## Development

```bash
make check
```
