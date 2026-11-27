# Iteration 2 — Protobuf Contracts, ACL, Internal Use-Case Commands & `eventsalsa/store`

In Iteration 2 of `with-event-sourcing`, external command (`commands/*.proto`) and query (`queries/*.proto`) contracts are compiled Protobuf packages at the top level, keeping wire schema evolution isolated from internal layers—echoing [`without-event-sourcing/iteration-2`](../../without-event-sourcing/iteration-2/) while persisting events via `eventsalsa/store` and `eventmap-gen`.

Each write flows through four layers:
1. **`internal/infra/acl`** translates external `*commands.*` Protobuf messages via `command.Func` into internal `application.*` structs.
2. **`internal/application`** defines internal use-case commands (`PlaceOrder`, `ConfirmOrder`) and write handlers orchestrating `domain.PlaceOrder` / `Load -> Confirm -> Save`.
3. **`internal/domain`** owns the OOP event-sourced `Order` aggregate (`PlaceOrder`, `Confirm`, `ReconstituteOrder`, `Evolve`, `MarkCommitted`) and `OrderRepository` (`Load`/`Save`), with zero `store`, `pgx`, or Protobuf imports.
4. **`internal/infra/{persistence,queries}`** handles PostgreSQL transactions (`WithPgxTx`), `eventmap-gen` mappings, optimistic concurrency (`store.Exact(order.Version())`), `OrderOverviewProjection`, and Protobuf SQL read queries.

## Package Layout

```text
iteration-2/
├── generate.go                                  # //go:generate protoc directives
├── main.go                                      # Thin entrypoint running sample commands & queries
├── bootstrap.go                                 # Composition root (pgxpool, store, ACL, middlewares, buses)
├── main_test.go                                 # End-to-end ACL/domain tests + nil-proto safety
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
    ├── domain/                                  # Event-sourced Order aggregate & OrderRepository port
    │   ├── errors.go
    │   ├── order.go
    │   ├── order_test.go
    │   ├── repository.go
    │   └── events/                              # Pure event structs scanned by eventmap-gen
    │       ├── order_placed.go
    │       └── order_confirmed.go
    ├── application/                             # Internal use-case commands & write handlers
    │   ├── place_order.go
    │   ├── place_order_handler.go
    │   ├── confirm_order.go
    │   └── confirm_order_handler.go
    └── infra/
        ├── acl/                                 # command.Func adapters translating proto -> application
        │   ├── errors.go
        │   ├── place_order_acl.go
        │   └── confirm_order_acl.go
        ├── persistence/                         # storepostgres.Store repository, tx, projection, eventmap-gen
        │   ├── generate.go                      # //go:generate eventmap-gen directive
        │   ├── event_mapping.gen.go
        │   ├── event_mapping.gen_test.go
        │   ├── tx.go
        │   ├── schema.go
        │   ├── projection.go
        │   └── order_repository.go
        └── queries/                             # Thin SQL read handler returning Protobuf views
            ├── errors.go
            └── get_order_overview_handler.go
```

## Run

```bash
# Run pure ACL/domain tests and generated mapping tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/iteration-2/...

# Regenerate .pb.go contracts and eventmap-gen bindings (requires protoc + protoc-gen-go)
go generate ./with-event-sourcing/iteration-2/...

# Run full PostgreSQL example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-2
```
