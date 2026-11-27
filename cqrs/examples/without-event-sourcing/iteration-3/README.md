# Iteration 3 — Protobuf Contracts, ACL, Generic Decider Runner & `spectest`

In Iteration 3, external Protobuf contracts and the `command.Func` ACL stay at the boundary, while write-side business rules move to a pure functional **Decider** (`InitialOrder`, `Decide*`, `EvolveOrder`) in `internal/domain`—even though persistence stores only current state without an event log.

Because every domain command shares the same `(Order, Cmd) -> ([]any, error)` decision signature, `internal/application` collapses from per-command handler structs down to a single generic `NewDeciderHandler` runner, and all domain behavior is spec-tested directly with `eventsalsa.dev/cqrs/spectest`.

## Package Layout

```text
iteration-3/
├── generate.go                              # //go:generate protoc directives
├── main.go                                  # Composition root (DB, ACL + Decider wiring, buses, run)
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
    ├── domain/                              # Functional Decider: state, commands, events, Decide/Evolve, repo port
    │   ├── errors.go
    │   ├── events.go                        # OrderPlaced, OrderCanceled facts
    │   ├── order.go                         # Order state, InitialOrder, EvolveOrder
    │   ├── place_order.go                   # Private PlaceOrder command + DecidePlaceOrder
    │   ├── cancel_order.go                  # Private CancelOrder command + DecideCancelOrder
    │   ├── repository.go                    # State-based Find/Save OrderRepository (no event store)
    │   └── decider_test.go                  # Given/When/Then domain specs via cqrs/spectest
    ├── application/                         # Generic Decider orchestration runner
    │   └── decider_handler.go               # Single NewDeciderHandler[C] eliminating per-command handlers
    └── infra/
        ├── persistence/                     # State DB adapter (Find/Save + read projections)
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

- Eliminates per-command `PlaceOrderHandler` / `CancelOrderHandler` boilerplate via one uniform pure Decider signature.
- Makes domain commands private to `internal/domain` cleanly, because `internal/domain` owns `DecidePlaceOrder` and `DecideCancelOrder`.
- Unlocks sub-millisecond `Given / When / Then` domain testing with [`eventsalsa.dev/cqrs/spectest`](../../../spectest/) even over non-event-sourced state storage.
- See [`../iteration-2/`](../iteration-2/) for classic entity methods with per-command application handlers.

## Run

```bash
go run ./without-event-sourcing/iteration-3
go test -race -count=1 ./without-event-sourcing/iteration-3/...
```

To regenerate the `.pb.go` files after modifying `.proto` contracts (`protoc` + `protoc-gen-go` required):

```bash
go generate ./without-event-sourcing/iteration-3/...
```
