# Iteration 1 — Direct Public Contracts, Domain Repository, `eventmap-gen` & `eventsalsa/store`

In Iteration 1 of `with-event-sourcing`, external command (`commands/`) and query (`queries/`) contracts are exported as plain Go structs at the top level—echoing [`without-event-sourcing/iteration-1`](../../without-event-sourcing/iteration-1/)—while write-side invariants and `OrderRepository` (`Load`/`Save`) live in `internal/domain`, write use cases live in `internal/application`, and PostgreSQL/`eventsalsa/store` persistence (with `eventmap-gen` generated event mappings) lives in `internal/infra`.

`Order` carries its unexported loaded stream version and uncommitted events internally so application handlers never handle event slices or stream versions directly—illustrating classic OOP event sourcing ahead of a later stateless Decider iteration with `eventsalsa.dev/cqrs/persistence`.

## Package Layout

```text
iteration-1/
├── main.go                              # Thin entrypoint running sample commands & queries
├── bootstrap.go                         # Composition root (pgxpool, store, schema, middlewares, buses)
├── main_test.go                         # Application/domain flow and query boundary tests (DB-free)
├── commands/                            # Public write command contracts
│   ├── place_order.go
│   └── confirm_order.go
├── queries/                             # Public query & projected view DTO contracts
│   ├── get_order_overview.go
│   └── order_overview.go
└── internal/
    ├── domain/                          # Order aggregate & repository port (zero store/pgx imports)
    │   ├── errors.go
    │   ├── order.go                     # Order, PlaceOrder, ReconstituteOrder, Evolve, Confirm
    │   ├── order_test.go                # Direct aggregate unit tests
    │   ├── repository.go                # OrderRepository interface (Load, Save)
    │   └── events/                      # Pure event structs scanned by eventmap-gen
    │       ├── order_placed.go
    │       └── order_confirmed.go
    ├── application/                     # Write-side use-case handlers (1 file per command)
    │   ├── place_order_handler.go
    │   └── confirm_order_handler.go
    └── infra/                           # PostgreSQL + eventsalsa/store adapter & read-side query handler
        ├── generate.go                  # //go:generate eventmap-gen directive
        ├── event_mapping.gen.go         # Generated ToESEvents / FromESEvent mappings
        ├── event_mapping.gen_test.go    # Generated mapping tests
        ├── tx.go                        # WithPgxTx middleware & tx context helpers
        ├── schema.go                    # eventsalsa/store DDL + order_overview_v1 table DDL
        ├── projection.go                # OrderOverviewProjection (projector.Projection shape, called inline)
        ├── order_repository.go          # OrderRepository backed by storepostgres.Store
        ├── order_repository_test.go     # Tx guard, no-op save, and projection error unit tests
        └── get_order_overview_handler.go# Thin SQL read handler querying order_overview_v1
```

## Run

```bash
# Run Order aggregate unit tests and generated mapping tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/iteration-1/...

# Regenerate event mappings if domain/events/* changes
go generate ./with-event-sourcing/iteration-1/...

# Run full PostgreSQL example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-1
```
