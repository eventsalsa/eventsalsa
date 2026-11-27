# eventsalsa/cqrs/persistence/postgres

PostgreSQL persistence adapters, transaction scoping, inline projection hooks, and causal event helpers for `eventsalsa.dev/cqrs/persistence`.

## Explicit Transaction Ownership (Zero Silent Inheritance)

Every transaction starter (`InReadTx`, `InTx`, `NewTxMiddleware`, `NewQueryTxMiddleware`, `AdoptTx`) is strictly **non-reentrant** and fails immediately with `ErrTxAlreadyActive` if a transaction is already active in `context.Context`. This enforces single ownership cleanly across two explicit patterns:

1. **Narrow transactions by default (`NewEventRepository`):**
   - `EventRepository.Load` runs inside a short `pgx.ReadOnly` transaction (`InReadTx`) that commits and releases the connection **before** the domain `decide` function runs.
   - `EventRepository.Append` runs inside a short read-write `pgx.Tx` (`InTx`) strictly around `store.EventStore.Append` and inline projections, relying on optimistic concurrency (`expectedVersion`) + `command.RetryMiddleware`.
   - Mixing a narrow `EventRepository` (or narrow `InReadTx`/`InTx` calls) inside `NewTxMiddleware` or `AdoptTx` fails loudly with `ErrTxAlreadyActive`.
2. **Wide transactions by composition (`NewTxMiddleware` + `NewTxEventRepository` or `LoadTx`/`AppendTx`):**
   - When cross-stream or multi-table SQL atomicity is needed, mount `MustTxMiddleware(pool)` / `NewTxMiddleware(pool)` on `command.Bus` (or wrap `ctx` via `AdoptTx`).
   - Pair it with **`NewTxEventRepository`** (which has no `DB` field, participates in `RequireTx(ctx)`, and fails with `ErrMissingTx` if no outer transaction is active) or explicit **`LoadTx(ctx, tx, id)` / `AppendTx(ctx, tx, ...)`** calls.

## Usage

### Narrow Event Sourcing (`eventsalsa/store` + `eventmap-gen`)

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

### Wide Transaction Composition (`MustTxMiddleware` + `NewTxEventRepository`)

```go
bus, err := command.NewBus(
	command.WithMiddleware(
		command.ObservabilityMiddleware(),
		command.RetryMiddleware(command.RetryConfig{
			MaxAttempts: 3,
			RetryIf:     postgres.IsConcurrencyConflict,
		}),
		postgres.MustTxMiddleware(pool),
	),
)

repo, err := postgres.NewTxEventRepository(postgres.TxEventRepositoryConfig[any]{
	EventStore:  es,
	StreamType:  "Order",
	Codec:       codec,
	Projections: []postgres.Projection{&OrderOverviewProjection{}},
})
```

### State-Based Persistence (`StateRepository[S]`)

Use `InReadTx` inside `Load` and `InTx` inside `Save` for narrow SQL state repositories, or `RequireTx(ctx)` when composing inside `MustTxMiddleware`:

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

### Event Causality Bridging

```go
// When reacting to a persisted event from eventsalsa/projector:
dispatchCtx := postgres.ContextWithEventCausation(ctx, persistedEvent)
```

## Development

```bash
# Format check, vet, lint, unit tests, PostgreSQL integration tests, gosec, and govulncheck
make check
```
