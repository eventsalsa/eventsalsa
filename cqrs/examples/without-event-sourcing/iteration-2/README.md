# Iteration 2 — Protobuf Contracts, ACL & Internal Use-Case Commands

In Iteration 2, external transport/API contracts live at the top level as generated Protobuf packages (`commands/` and `queries/`), keeping wire schema evolution isolated from internal packages.

Each write flows cleanly through three responsibilities:
1. **`internal/infra/acl`** translates external `*commands.*` Protobuf payloads on `command.Bus` into internal use-case commands (`application.*`) using `command.Func`.
2. **`internal/application`** defines internal use-case commands (`PlaceOrder`, `CancelOrder`) and write handlers that orchestrate `Load -> Domain -> Save`.
3. **`internal/domain`** owns the rich `Order` entity (`NewOrder`, `(*Order).Cancel`), business invariants, and write repository port (`OrderRepository`), with zero transport or use-case DTO leakage.

## Package Layout

```text
iteration-2/
├── generate.go                              # //go:generate protoc directives
├── main.go                                  # Thin entrypoint running sample commands & queries
├── bootstrap.go                             # Composition root (DB, ACL adapters, middlewares, buses)
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
    ├── domain/                              # Entity, invariants, errors, repository port (zero command structs)
    │   ├── errors.go
    │   ├── order.go
    │   ├── order_test.go                    # Mock-free tests for NewOrder & (*Order).Cancel
    │   └── repository.go
    ├── application/                         # Internal use-case commands & orchestrating write handlers
    │   ├── place_order.go
    │   ├── place_order_handler.go
    │   ├── cancel_order.go
    │   └── cancel_order_handler.go
    └── infra/
        ├── persistence/                     # Database adapter implementing write port & read projections
        │   └── memory_db.go
        ├── acl/                             # command.Func ACL adapters translating proto -> application
        │   ├── errors.go
        │   ├── place_order_acl.go
        │   └── cancel_order_acl.go
        └── queries/                         # Thin SQL-like read handlers returning Protobuf views
            ├── errors.go
            └── get_order_summary_handler.go
```

## Why choose Iteration 2

- Isolates wire/schema evolution (`proto3`) from internal use-case contracts and domain models.
- Uses `command.Func` cleanly at the infra boundary (`internal/infra/acl`) without putting orchestration or business rules inside ACL code.
- Keeps `internal/domain` purely focused on entity state, invariants, and repository ports.
- See [`../iteration-1/`](../iteration-1/) if you want plain Go structs with zero translation boilerplate.
- See [`../iteration-3/`](../iteration-3/) for a pure `internal/domain` Decider + `eventsalsa.dev/cqrs/persistence` (`FromStateRepository`, `NewCommandHandler`) tested via `cqrs/spectest`.

## Run

```bash
go run ./without-event-sourcing/iteration-2
go test -race -count=1 ./without-event-sourcing/iteration-2/...
```

To regenerate the `.pb.go` files after modifying `.proto` contracts (`protoc` + `protoc-gen-go` required):

```bash
go generate ./without-event-sourcing/iteration-2/...
```
