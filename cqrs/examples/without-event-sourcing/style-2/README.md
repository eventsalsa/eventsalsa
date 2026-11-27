# Style 2 — Protobuf Contracts, ACL, Thin Application & Pure Domain Handlers

In Style 2, transport/API contracts live at the top level as generated Protobuf packages (`commands/` and `queries/`), while domain commands (`internal/domain`) remain private inside the module boundary and are handled directly by pure, side-effect-free domain functions.

Each write flows through three distinct responsibilities:
1. **`internal/infra/acl`** translates external `*commands.*` Protobuf payloads on `command.Bus` into private `domain.*` commands using `command.Func`.
2. **`internal/application`** provides thin use-case orchestration (`Load -> Domain -> Save`), keeping repository/I/O coordination out of both `domain` and `infra`.
3. **`internal/domain`** executes pure, side-effect-free command handlers (`HandlePlaceOrder`, `HandleCancelOrder`) over `Order` state and returns updated state values without mutating callers or touching persistence.

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
    ├── domain/                              # State, invariants, private commands, pure handlers, repo port
    │   ├── errors.go
    │   ├── order.go
    │   ├── order_test.go                    # Mock-free tests for pure domain handlers
    │   ├── place_order.go                   # PlaceOrder + pure HandlePlaceOrder
    │   ├── cancel_order.go                  # CancelOrder + pure HandleCancelOrder
    │   └── repository.go
    ├── application/                         # Thin Load -> Domain -> Save orchestration
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
- Keeps domain commands private to `internal/` and interpreted purely inside `internal/domain` without side effects or mocks.
- Keeps orchestration (`Get -> Domain -> Save`) explicitly in `internal/application` rather than leaking use-case coordination into `internal/infra/acl`.
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
