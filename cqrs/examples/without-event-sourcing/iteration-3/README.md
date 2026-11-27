# Iteration 3 — Protobuf Contracts, ACL, `eventsalsa.dev/cqrs/persistence` Decider & `spectest`

In Iteration 3, external Protobuf contracts and the `command.Func` ACL stay at the boundary, while write-side business rules live in a pure functional **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) inside `internal/domain`—even though persistence stores only current state without an event log.

Rather than duplicating per-command handler files or bespoke generic glue, `internal/bootstrap` adapts [`internal/infra/persistence/memory_db.go`](./internal/infra/persistence/memory_db.go) (`persistence.StateRepository[domain.Order]`) cleanly via [`eventsalsa.dev/cqrs/persistence`](../../../persistence/) (`persistence.FromStateRepository` + `persistence.NewCommandHandler`). For single-aggregate commands with no cross-service orchestration, `internal/application` disappears completely (returning only if a use case later needs impure multi-aggregate coordination).

## Package Layout

```text
iteration-3/
├── generate.go                              # //go:generate protoc directives
├── main.go                                  # Thin entrypoint running sample commands & queries
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
    ├── bootstrap/                           # Composition root (DB, cqrs/persistence adapter, ACL, buses)
    │   └── bootstrap.go
    ├── domain/                              # Pure Functional Decider: state, commands, events, Decide/Evolve
    │   ├── errors.go
    │   ├── events.go                        # OrderPlaced, OrderCanceled facts
    │   ├── order.go                         # Order state, InitialOrder, EvolveOrder
    │   ├── place_order.go                   # Private PlaceOrder command + DecidePlaceOrder
    │   ├── cancel_order.go                  # Private CancelOrder command + DecideCancelOrder
    │   └── decider_test.go                  # Given/When/Then domain specs via cqrs/spectest
    └── infra/
        ├── persistence/                     # MemoryDB implementing persistence.StateRepository[domain.Order]
        │   └── memory_db.go
        ├── acl/                             # command.Func ACL adapters translating proto -> domain
        │   ├── errors.go
        │   ├── place_order_acl.go
        │   └── cancel_order_acl.go
        └── queries/                         # Thin SQL-like read handlers returning Protobuf views
            ├── errors.go
            └── get_order_summary_handler.go
```

## Why choose Iteration 3

- Eliminates per-command handler boilerplate via [`eventsalsa.dev/cqrs/persistence`](../../../persistence/) (`FromStateRepository`, `NewCommandHandler`).
- Keeps `internal/domain` 100% pure—no repository interface, no mutating entity methods, and zero persistence imports.
- Unlocks sub-millisecond `Given / When / Then` domain testing with [`eventsalsa.dev/cqrs/spectest`](../../../spectest/) even over non-event-sourced state storage.
- See [`../iteration-2/`](../iteration-2/) for classic entity methods with explicit `internal/application` orchestration handlers.

## Run

```bash
go run ./without-event-sourcing/iteration-3
go test -race -count=1 ./without-event-sourcing/iteration-3/...
```

To regenerate the `.pb.go` files after modifying `.proto` contracts (`protoc` + `protoc-gen-go` required):

```bash
go generate ./without-event-sourcing/iteration-3/...
```
