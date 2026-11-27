# Iteration 1 — Direct Public Contracts, Application Handlers, `eventmap-gen` & `eventsalsa/store`

In Iteration 1 of `with-event-sourcing`, external command (`commands/`) and query (`queries/`) contracts are exported as plain Go structs at the top level—echoing [`without-event-sourcing/iteration-1`](../../without-event-sourcing/iteration-1/)—while write rules, application orchestration, and PostgreSQL/`eventsalsa/store` persistence (with `eventmap-gen` generated event mappings) stay separated under `internal/`.

## Package Layout

```text
iteration-1/
├── main.go                              # Composition root (pgxpool, store, schema, buses, run)
├── commands/                            # Public write command contracts
│   ├── place_order.go
│   └── confirm_order.go
├── queries/                             # Public query & projected view DTO contracts
│   ├── get_order_overview.go
│   └── order_overview.go
└── internal/
    ├── domain/                          # Pure OrderState & decision rules (zero store/pgx imports)
    │   ├── errors.go
    │   ├── order.go                     # OrderState, InitialOrder, EvolveOrder, Place, Confirm
    │   ├── order_test.go                # Given/When/Then specs via cqrs/spectest
    │   └── events/                      # Pure event structs scanned by eventmap-gen
    │       ├── order_placed.go
    │       └── order_confirmed.go
    ├── application/                     # Write-side use-case handlers + store port (1 file per command)
    │   ├── order_event_store.go         # OrderEventStore interface (Load, Append)
    │   ├── place_order_handler.go
    │   └── confirm_order_handler.go
    └── infra/                           # PostgreSQL + eventsalsa/store adapter & read-side query handler
        ├── generate.go                  # //go:generate eventmap-gen directive
        ├── event_mapping.gen.go         # Generated ToESEvents / FromESEvent mappings
        ├── event_mapping.gen_test.go    # Generated mapping tests
        ├── tx.go                        # WithPgxTx middleware & tx context helpers
        ├── schema.go                    # eventsalsa/store DDL + order_overview_v1 table DDL
        ├── projection.go                # Inline SQL projection updating order_overview_v1 in tx
        ├── order_event_store.go         # OrderEventStore backed by storepostgres.Store
        └── get_order_overview_handler.go# Thin SQL read handler querying order_overview_v1
```

## Run

```bash
# Run pure Given/When/Then domain specs and generated mapping tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/iteration-1/...

# Regenerate event mappings if domain/events/* changes
go generate ./with-event-sourcing/iteration-1/...

# Run full PostgreSQL example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-1
```
