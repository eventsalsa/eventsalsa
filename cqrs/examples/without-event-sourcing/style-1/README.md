# Style 1 — Direct Public Contracts & Application Handlers

In Style 1, the module exports Go command structs (`commands/`) and query/DTO structs (`queries/`) directly at the top level while keeping business rules, write handlers, and persistence hidden under `internal/`.

## Package Layout

```text
style-1/
├── main.go                              # Composition root (DB, buses, handler wiring, run)
├── main_test.go                         # End-to-end tests
├── commands/                            # Public write command contracts
│   ├── place_order.go
│   └── cancel_order.go
├── queries/                             # Public query & view DTO contracts
│   ├── get_order_summary.go
│   └── order_summary.go
└── internal/
    ├── domain/                          # Business entity, invariants, errors, write repository port
    │   ├── errors.go
    │   ├── order.go
    │   └── repository.go
    ├── application/                     # Write-side use case handlers (1 file per command)
    │   ├── place_order_handler.go
    │   └── cancel_order_handler.go
    └── infra/                           # Persistence adapter + thin SQL-like read handler
        ├── memory_db.go
        └── get_order_summary_handler.go
```

## Why choose Style 1

- Zero translation boilerplate between external callers and application handlers.
- `internal/application` imports `commands` and `internal/domain`—it never imports `queries` or `internal/infra`, enforcing CQRS separation at compile time.
- Works best when internal Go contracts are already stable or owned within one team/monorepo. See [`../style-2/`](../style-2/) when transport contracts (e.g., Protobuf) must evolve independently from domain commands.

## Run

```bash
go run ./without-event-sourcing/style-1
go test -race -count=1 ./without-event-sourcing/style-1/...
```
