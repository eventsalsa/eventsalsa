# Style 2 — Protobuf Contracts, Private Domain Commands & Infra ACL

In Style 2, transport/API contracts live at the top level as generated Protobuf packages (`commands/` and `queries/`), while domain commands (`internal/domain`) remain private inside the module boundary.

An anti-corruption layer (ACL) in `internal/infra/acl` adapts incoming `*commands.*` Protobuf messages on `command.Bus` into `domain.*` commands using `command.Func` and delegates to `internal/application` write handlers.

## Package Layout

```text
style-2/
├── generate.go                              # //go:generate protoc directives
├── main.go                                  # Composition root (DB, ACL adapters, buses, run)
├── main_test.go                             # End-to-end tests + nil-proto ACL safety
├── commands/                                # Public Protobuf command contracts
│   ├── place_order.proto
│   ├── place_order.pb.go
│   ├── cancel_order.proto
│   └── cancel_order.pb.go
├── queries/                                 # Public Protobuf query & view contracts
│   ├── get_order_summary.proto
│   ├── get_order_summary.pb.go
│   ├── order_summary.proto
│   └── order_summary.pb.go
└── internal/
    ├── domain/                              # Business entity, invariants, domain commands, repo port
    │   ├── errors.go
    │   ├── order.go
    │   ├── place_order.go
    │   ├── cancel_order.go
    │   └── repository.go
    ├── application/                         # Write-side use case handlers over domain commands
    │   ├── place_order_handler.go
    │   └── cancel_order_handler.go
    └── infra/
        ├── persistence/                     # Database adapter implementing write port & read projections
        │   └── memory_db.go
        ├── acl/                             # command.Func ACL adapters translating proto -> domain
        │   ├── place_order_acl.go
        │   └── cancel_order_acl.go
        └── queries/                         # Thin SQL-like read handlers returning Protobuf views
            └── get_order_summary_handler.go
```

## Why choose Style 2

- Isolates wire/schema evolution (`proto3`) from internal domain models and command payloads.
- Keeps domain commands private to `internal/` so callers cannot construct internal domain state transitions directly.
- Uses `command.Func` cleanly at the infra boundary (`internal/infra/acl`) without hiding write business rules inside transport code.
- See [`../style-1/`](../style-1/) if you want plain Go structs with zero translation boilerplate.

## Run

```bash
go run ./without-event-sourcing/style-2
go test -race -count=1 ./without-event-sourcing/style-2/...
```

To regenerate the `.pb.go` files after modifying `.proto` contracts (`protoc` + `protoc-gen-go` required):

```bash
go generate ./without-event-sourcing/style-2/...
```
