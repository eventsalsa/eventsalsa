# Iteration 4 — Snapshotted Event Sourcing (`eventsalsa/snapshot` + `postgres.NewSnapshotAdapter`)

In Iteration 4 of `with-event-sourcing`, streams grow across repeating `AddOrderItem` (`OrderItemAdded`) commands and pair the pure **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) with automatic compressed stream snapshots from [`eventsalsa.dev/snapshot`](../../../../snapshot/).

Instead of changing `internal/domain` or `internal/infra/acl` when adding snapshotting, [`bootstrap.go`](./bootstrap.go) switches only the persistence adapter constructed for `registerCommandHandlers` to [`postgres.NewSnapshotAdapter`](../../../persistence/postgres/) (`snapshotpostgres.NewStore`, `snapshot.JSON[domain.Order]()`, `snapshot.EveryNEvents(3)`, `snapshot.Gzip()`, and state-version parity verification), returning the exact same `persistence.Adapter[domain.Order, any]` consumed by `persistence.NewCommandHandler`.

## Package Layout

```text
iteration-4/
├── generate.go                                  # //go:generate protoc directives
├── main.go                                      # Entrypoint exercising PlaceOrder, 2x AddOrderItem, ConfirmOrder, and query
├── bootstrap.go                                 # Composition root wiring postgres.NewSnapshotAdapter + eventsalsa/snapshot
├── main_test.go                                 # End-to-end flow, OCC retry, and nil-proto ACL/query tests (DB-free)
├── commands/                                    # Public Protobuf command contracts
│   ├── place_order.proto / .pb.go
│   ├── add_order_item.proto / .pb.go
│   └── confirm_order.proto / .pb.go
├── queries/                                     # Public Protobuf query & projected view contracts
│   ├── get_order_overview.proto / .pb.go
│   └── order_overview.proto / .pb.go
└── internal/
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
        ├── persistence/                         # store + snapshot DDL, eventmap-gen codec, order_overview_v1 projection
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
