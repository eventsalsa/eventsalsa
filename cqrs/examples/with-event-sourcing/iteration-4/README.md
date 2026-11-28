# Iteration 4 — Snapshotted Event Sourcing (`eventsalsa/snapshot` + `postgres.NewSnapshotAdapter`)

In Iteration 4 of `with-event-sourcing`, streams grow across repeating `AddOrderItem` (`OrderItemAdded`) commands and pair the pure **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) with automatic compressed stream snapshots from [`eventsalsa.dev/snapshot`](../../../../snapshot/).

Each layer has a clear owner:

- [`config/`](./config/): Top-level runtime configuration (`Config`, `SnapshotConfig`, `RetryConfig`), environment loading (`LoadEnv` / `FromEnv`), and `Validate`.
- [`bootstrap/`](./bootstrap/): Dedicated composition root (`Open`, `Close`, `NewCommandBus`, `RegisterOrderCommands`, `NewQueryBus`) wiring pools, stores, `postgres.IsConcurrencyConflict` retries, `domain.IsRejection` + `acl.ErrEmptyCommand` classification, and ACL command translators.
- [`internal/infra/persistence/order_snapshot_adapter.go`](./internal/infra/persistence/order_snapshot_adapter.go): Functional-option builder (`NewOrderSnapshotAdapter`) pairing `OrderDecider`, `eventmap-gen` codecs, `OrderOverviewProjection`, and [`postgres.NewSnapshotAdapter`](../../../persistence/postgres/) (`snapshotpostgres.NewStore`, `snapshot.JSON[domain.Order]()`, `snapshot.EveryNEvents(3)`, `snapshot.Gzip()`, state-version parity verification).

## Package Layout

```text
iteration-4/
├── generate.go                                  # //go:generate protoc directives
├── main.go                                      # Thin entrypoint calling config.LoadEnv + bootstrap.Open
├── config/                                      # Top-level runtime config, env loading, and validation
│   ├── config.go
│   └── config_test.go
├── bootstrap/                                   # Composition root & bus wiring
│   ├── bootstrap.go                             # App container (Open, Close)
│   ├── command_bus.go                           # NewCommandBus, RegisterOrderCommands, error classification
│   ├── query_bus.go                             # NewQueryBus
│   └── bootstrap_test.go                        # Bus/ACL/OCC/guard unit tests
├── commands/                                    # Public Protobuf command contracts
│   ├── place_order.proto / .pb.go
│   ├── add_order_item.proto / .pb.go
│   └── confirm_order.proto / .pb.go
├── queries/                                     # Public Protobuf query & projected view contracts
│   ├── get_order_overview.proto / .pb.go
│   └── order_overview.proto / .pb.go
└── internal/
    ├── domain/                                  # Pure Functional Decider (zero store/snapshot/pgx/proto/cqrs imports)
    │   ├── errors.go                            # Sentinels + pure domain.IsRejection predicate
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
        ├── acl/                                 # command.Func ACL translators (Protobuf -> domain commands)
        ├── persistence/                         # store + snapshot DDL, eventmap-gen, OrderOverviewProjection, NewOrderSnapshotAdapter
        │   ├── order_snapshot_adapter.go
        │   ├── order_snapshot_adapter_test.go
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
