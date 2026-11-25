# `eventsalsa.dev/cqrs`

Minimal, architecture-neutral Go CQRS primitives with zero database or transport coupling.

## Highlights

- **Zero DB / Broker Coupling:** Depends only on the Go standard library and `github.com/google/uuid` — zero `pgx`, `store`, `snapshot`, `projector`, or messaging dependencies.
- **Typed Endpoints + Composable Middlewares:** Define explicit `CommandHandler[C]` (`Handle(ctx, cmd) error`) and `QueryHandler[Q, R]` (`Handle(ctx, query) (R, error)`), wrapped cleanly with non-generic `CommandMiddleware` and `QueryMiddleware` chains (`WrapCommand`, `WrapQuery`).
- **Optional Lightweight Buses (`CommandBus`, `QueryBus`):** Dispatch dynamically when decoupling callers or handling polymorphic command lists without forcing buses onto teams who prefer direct function calls. Query calls infer `Q` automatically (`cqrs.Query[Profile](ctx, bus, GetProfile{ID: "1"})`).
- **Deterministic Boundary Opacity on `CommandBus`:** Distinguishes pre-dispatch failures, unhandled infrastructure errors, and marked domain rejections (`cqrs.Reject` / `WithCommandErrorClassifier`) with explicit policies: default `MaskRejections`, `WithStrictOpaqueDispatch`, or `WithPassthroughErrors`.
- **Context Causation & Backoff Retries:** `CausationMiddleware` links `TraceID`, `CommandID`, and parent `CausationID`; `RetryCommandMiddleware` provides jittered exponential retries controlled explicitly by `RetryIf`.
- **Zero-DB BDD Specs (`cqrs/spectest`):** Test pure `Initial`, `Evolve`, and domain decision functions in sub-millisecond `Given / When / Then` specs.

## Installation

```sh
go get eventsalsa.dev/cqrs
```
