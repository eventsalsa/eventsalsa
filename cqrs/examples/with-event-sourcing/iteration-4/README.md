# Iteration 4 — Snapshotted Event Sourcing (`eventsalsa/snapshot` + `postgres.NewSnapshotAdapter`)

In Iteration 4 of `with-event-sourcing`, streams grow across repeating `AddOrderItem` (`OrderItemAdded`) commands and pair the pure **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) with automatic compressed stream snapshots from [`eventsalsa.dev/snapshot`](../../../../snapshot/).

Instead of mixing environment settings, codec/projection wiring, bus middlewares, and storage bootstrapping into one file, `iteration-4` cleanly splits each responsibility:

- [`internal/config/`](./internal/config/): Typed runtime configuration (`Config`, `SnapshotConfig`, `RetryConfig`), environment loading (`LoadEnv` / `FromEnv`), and validation (`Validate`).
- [`internal/infra/persistence/order_adapter.go`](./internal/infra/persistence/order_adapter.go): Storage adapter assembly pairing `OrderDecider`, `eventmap-gen` codecs, `OrderOverviewProjection`, and [`postgres.NewSnapshotAdapter`](../../../persistence/postgres/) (`snapshotpostgres.NewStore`, `snapshot.JSON[domain.Order]()`, `snapshot.EveryNEvents(3)`, `snapshot.Gzip()`, state-version parity verification).
- [`app.go`](./app.go): Application lifecycle (`openApp`, `close`), `pgxpool` connection setup, schema migration installation, and concrete store initialization.
- [`buses.go`](./buses.go): Command/query bus construction, retry/rejection policies, and Decider-to-ACL handler registration over `persistence.Adapter[domain.Order, any]`.

## Package Layout

```text
iteration-4/
├── generate.go                                  # //go:generate protoc directives
├── main.go                                      # Entrypoint loading env config and running commands & query
├── app.go                                       # App container lifecycle (pgxpool, schema, stores, adapter, buses)
├── buses.go                                     # Command & query bus middlewares, classifiers, and ACL handler binding
├── main_test.go                                 # End-to-end flow, OCC retry, config guard, and nil-proto ACL/query tests
├── commands/                                    # Public Protobuf command contracts
│   ├── place_order.proto / .pb.go
│   ├── add_order_item.proto / .pb.go
│   └── confirm_order.proto / .pb.go
├── queries/                                     # Public Protobuf query & projected view contracts
│   ├── get_order_overview.proto / .pb.go
│   └── order_overview.proto / .pb.go
└── internal/
    ├── config/                                  # Runtime Config, Default/FromEnv loading, and validation
    │   ├── config.go
    │   └── config_test.go
    ├── domain/                                  # Pure Functional Decider (zero store/snapshot/pgx/proto imports)
    │   ├── errors.go
    │   ├── order.go                             # Order state with JSON tags & Version parity counter
    │   ├── place_order.go
    │   ├── add_order_item.go
    │   ├── confirm_order.go
    │   ├── decider_test.go                      # Given/When/Then domain specs via cqrs/spectest
    │   └── events/
    │       ├── order_placed.go
    │       ├── order_item_added.go
    │       └── order_confirmed.go
    └── infra/
        ├── acl/                                 # command.Func adapters translating proto -> domain commands
        ├── persistence/                         # store + snapshot DDL, eventmap-gen, OrderOverviewProjection, NewOrderAdapter
        │   ├── order_adapter.go
        │   ├── order_adapter_test.go
        │   ├── generate.go
        │   ├── event_mapping.gen.go / _test.go
        │   ├── schema.go
        │   ├── projection.go
        │   └── projection_test.go
        └── queries/                             # Thin SQL read handler returning Protobuf views
```

## Run

```bash
# Run pure Decider specs, ACL/OCC tests, and generated mapping tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/iteration-4/...

# Regenerate .pb.go contracts and eventmap-gen bindings (requires protoc + protoc-gen-go)
go generate ./with-event-sourcing/iteration-4/...

# Run full PostgreSQL + snapshots example (creates store, snapshots, and projection tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-4
```
