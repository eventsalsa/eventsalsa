# eventsalsa/cqrs/persistence

Database-agnostic `Decider`, `StateRepository`, `EventRepository`, `Adapter`, and generic `NewCommandHandler` wiring for `eventsalsa.dev/cqrs`.

Depends strictly on `eventsalsa.dev/cqrs` and Go's standard library (zero database or driver imports). PostgreSQL-native `pgx/v5` transaction scoping and `eventsalsa/store` `EventRepository` wiring live in [`eventsalsa.dev/cqrs/persistence/postgres`](./postgres/).

## Usage

Both storage models bind `(repo, decider)` into one `Adapter[S, E]` and register handlers with the same `NewCommandHandler` constructor—letting multiple commands share one adapter and keeping command wiring identical across state-based and event-sourced backends.

### With State Persistence (`StateRepository[S]`)

```go
decider := persistence.Decider[domain.Order, any]{
	Initial: domain.InitialOrder,
	Evolve:  domain.EvolveOrder,
}
orders := persistence.FromStateRepository(repo, decider)

placeHandler, err := persistence.NewCommandHandler(
	orders,
	func(cmd domain.PlaceOrder) string { return cmd.OrderID },
	domain.DecidePlaceOrder,
)
```

`StateRepository` carries optimistic concurrency via `(version int64, expectedVersion int64)`: `Load` returns `ErrNotFound` when missing (causing `Save` to receive `expectedVersion == 0`), and updates receive the loaded `version` so concurrent writes fail with `ErrConcurrencyConflict`.

### With Event-Sourced Persistence (`EventRepository[E]`)

```go
decider := persistence.Decider[domain.Order, any]{
	Initial: domain.InitialOrder,
	Evolve:  domain.EvolveOrder,
}
orders := persistence.FromEventRepository(eventRepo, decider)

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
