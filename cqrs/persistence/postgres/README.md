# eventsalsa/cqrs/persistence/postgres

PostgreSQL persistence adapters, transaction scoping, inline projection hooks, and causal event helpers for `eventsalsa.dev/cqrs/persistence`.

## Transaction Model (Narrow by Default, Wide by Composition)

- **Narrow transactions by default:** `EventRepository.Load` (and `InReadTx`) reads state inside a short `pgx.ReadOnly` transaction that commits and releases the connection **before** the domain `decide` function runs. `EventRepository.Append` (and `InTx`) opens a short read-write `pgx.Tx` strictly around the database write and inline projections, relying on optimistic concurrency (`expectedVersion`) + `command.RetryMiddleware`.
- **Optional wide transactions:** When cross-stream atomicity or read-your-writes isolation across multiple steps is required, mount `MustTxMiddleware(pool)` / `NewTxMiddleware(pool)` on `command.Bus` (or adopt an outer `pgx.Tx` via `AdoptTx`). `InReadTx` and `InTx` automatically reuse any active transaction carried by `context.Context`.
- **Strictly non-reentrant starters:** Calling `NewTxMiddleware`, `NewQueryTxMiddleware`, or `AdoptTx` when a transaction is already active in `context.Context` fails loudly with `ErrTxAlreadyActive`.

## Usage

### Event Sourcing (`eventsalsa/store` + `eventmap-gen`)

```go
es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
codec, err := postgres.NewEventMapCodec(MarshalEvent, FromESEventAs[any])
if err != nil {
	return err
}

repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
	DB:          pool,
	EventStore:  es,
	StreamType:  "Order",
	Codec:       codec,
	Projections: []postgres.Projection{&OrderOverviewProjection{}},
})
if err != nil {
	return err
}

orders := persistence.FromEventRepository(repo, persistence.Decider[domain.Order, any]{
	Initial: domain.InitialOrder,
	Evolve:  domain.EvolveOrder,
})
placeHandler := persistence.MustNewCommandHandler(
	orders,
	func(cmd domain.PlaceOrder) string { return cmd.OrderID },
	domain.DecidePlaceOrder,
)
```

### State-Based Persistence (`StateRepository[S]`)

Use `InReadTx` inside `Load` and `InTx` inside `Save` so SQL state repositories share the exact same narrow-by-default / optional-wide transaction behavior:

```go
func (r *OrderRepo) Load(ctx context.Context, id string) (domain.Order, int64, error) {
	var (
		order domain.Order
		ver   int64
	)
	err := postgres.InReadTx(ctx, r.pool, func(ctx context.Context, tx pgx.Tx) error {
		return r.scanOrder(ctx, tx, id, &order, &ver)
	})
	return order, ver, err
}

func (r *OrderRepo) Save(ctx context.Context, id string, expectedVersion int64, order domain.Order) error {
	return postgres.InTx(ctx, r.pool, func(ctx context.Context, tx pgx.Tx) error {
		return r.upsertOrder(ctx, tx, id, expectedVersion, order)
	})
}
```

### Optimistic Concurrency Retries & Event Causality

```go
command.WithMiddleware(
	command.ObservabilityMiddleware(),
	command.RetryMiddleware(command.RetryConfig{
		MaxAttempts: 3,
		RetryIf:     postgres.IsConcurrencyConflict,
	}),
)

// When reacting to a persisted event from eventsalsa/projector:
dispatchCtx := postgres.ContextWithEventCausation(ctx, persistedEvent)
```

## Development

```bash
# Format check, vet, lint, unit tests, PostgreSQL integration tests, gosec, and govulncheck
make check
```
