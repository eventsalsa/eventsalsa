# Iteration 3 — Protobuf Contracts, ACL, `eventsalsa.dev/cqrs/persistence` Decider & `spectest`

In Iteration 3 of `with-event-sourcing`, external Protobuf contracts (`commands/*.proto`, `queries/*.proto`) and the `command.Func` ACL stay at the boundary, while write-side business rules move to a pure functional **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) in `internal/domain` tested with `eventsalsa.dev/cqrs/spectest`.

Rather than maintaining aggregate `Version`/`uncommitted`/`MarkCommitted` plumbing, per-command `internal/application` handler files, or manual `pgx` transaction/store glue, `bootstrap.go` wires [`eventsalsa.dev/cqrs/persistence/postgres`](../../../persistence/postgres/) (`postgres.NewEventRepository` + `postgres.NewEventMapCodec` + narrow-by-default read/write transactions over `eventsalsa/store` and `OrderOverviewProjection`) into [`eventsalsa.dev/cqrs/persistence`](../../../persistence/) (`persistence.FromEventRepository` + `persistence.NewCommandHandler`).

## Package Layout

```text
iteration-3/
├── generate.go                                  # //go:generate protoc directives
├── main.go                                      # Thin entrypoint running sample commands & queries
├── bootstrap.go                                 # Composition root (pgxpool, store, postgres EventRepository, cqrs/persistence adapter, ACL, buses)
├── main_test.go                                 # End-to-end flow, OCC retry, and nil-proto ACL tests (DB-free)
├── commands/                                    # Public Protobuf command contracts
│   ├── place_order.proto
│   ├── place_order.pb.go
│   ├── confirm_order.proto
│   └── confirm_order.pb.go
├── queries/                                     # Public Protobuf query & projected view contracts
│   ├── get_order_overview.proto
│   ├── get_order_overview.pb.go
│   ├── order_overview.proto
│   └── order_overview.pb.go
└── internal/
    ├── domain/                                  # Pure Functional Decider: state, commands, Decide/Evolve
    │   ├── errors.go
    │   ├── order.go                             # Order state, InitialOrder, EvolveOrder
    │   ├── place_order.go                       # Internal PlaceOrder command + DecidePlaceOrder
    │   ├── confirm_order.go                     # Internal ConfirmOrder command + DecideConfirmOrder
    │   ├── decider_test.go                      # Given/When/Then domain specs via cqrs/spectest
    │   └── events/                              # Pure event structs scanned by eventmap-gen
    │       ├── order_placed.go
    │       └── order_confirmed.go
    └── infra/
        ├── acl/                                 # command.Func adapters translating proto -> domain commands
        │   ├── errors.go
        │   ├── place_order_acl.go
        │   └── confirm_order_acl.go
        ├── persistence/                         # DDL, eventmap-gen codec, and inline order_overview_v1 projection
        │   ├── generate.go                      # //go:generate eventmap-gen directive
        │   ├── event_mapping.gen.go
        │   ├── event_mapping.gen_test.go
        │   ├── schema.go
        │   ├── projection.go
        │   └── projection_test.go               # Projection metadata and invalid payload tests
        └── queries/                             # Thin SQL read handler returning Protobuf views
            ├── errors.go
            └── get_order_overview_handler.go
```

## Run

```bash
# Run pure Decider specs, ACL/OCC tests, and generated mapping tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/iteration-3/...

# Regenerate .pb.go contracts and eventmap-gen bindings (requires protoc + protoc-gen-go)
go generate ./with-event-sourcing/iteration-3/...

# Run full PostgreSQL example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-3
```
