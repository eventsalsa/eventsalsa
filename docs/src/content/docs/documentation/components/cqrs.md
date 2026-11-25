---
title: CQRS
description: Transport-free command and query handlers, composable middlewares, optional buses, causation propagation, retries, and Given/When/Then specs with eventsalsa/cqrs.
---

`eventsalsa/cqrs` provides lightweight, architecture-neutral Go primitives for Command and Query Responsibility Segregation (CQRS).

In an event-sourced application, write operations validate intent and append facts, while read operations serve optimized read models. `eventsalsa/cqrs` focuses purely on the application boundary between callers and those handlers: strongly typed command and query contracts, composable non-generic middlewares, optional dynamic buses with explicit error policies, context-scoped causation propagation, jittered retries, and a zero-database BDD testing harness (`cqrs/spectest`).

The module is completely standalone. It depends only on the Go standard library and `github.com/google/uuid`, with zero dependencies on `pgx`, `eventsalsa/store`, `eventsalsa/projector`, or external brokers. You can adopt it alongside the rest of the eventsalsa stack or drop it into any Go service.

:::note
`eventsalsa.dev/cqrs` is in the `0.0.x` pre-release series while its ergonomics settle. Expect patch releases rather than `v0.1.0+` stability guarantees during this phase.
:::

### Package overview

| Package | Role |
| :--- | :--- |
| `eventsalsa.dev/cqrs` | `CommandHandler`, `QueryHandler`, `WrapCommand` / `WrapQuery`, optional `CommandBus` / `QueryBus`, `Reject` / `IsRejected`, `CausationMiddleware`, and `RetryCommandMiddleware` |
| `eventsalsa.dev/cqrs/spectest` | Zero-database `Given / When / Then` BDD test harness for pure `initial`, `evolve`, and `decide` functions |

## Installation

Add the module to your Go project (`Go 1.27.4+` required):

```bash
go get eventsalsa.dev/cqrs
```

No database migrations or background workers are required.

## Direct handlers vs. optional buses

`eventsalsa/cqrs` does not force a central bus onto every service. At the core are two generic interfaces and function adapters:

- `CommandHandler[C]` exposes `Handle(ctx context.Context, cmd C) error`, adapted from plain functions via `CommandFunc[C]`.
- `QueryHandler[Q, R]` exposes `Handle(ctx context.Context, query Q) (R, error)`, adapted from plain functions via `QueryFunc[Q, R]`.

If your HTTP handlers or workers prefer calling strongly typed services directly, decorate a handler once at startup with `WrapCommand` or `WrapQuery` and inject the resulting `CommandHandler[C]` or `QueryHandler[Q, R]` straight into your transport layer:

```go
package main

import (
	"context"
	"log/slog"

	"eventsalsa.dev/cqrs"
)

type PlaceOrder struct {
	OrderID    string
	CustomerID string
}

type GetOrder struct {
	OrderID string
}

type OrderView struct {
	OrderID string
	Status  string
}

func buildHandlers(logger *slog.Logger) (cqrs.CommandHandler[PlaceOrder], cqrs.QueryHandler[GetOrder, OrderView], error) {
	placeHandler := cqrs.CommandFunc[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		// Validate command, load stream, append events, etc.
		return nil
	})

	wrappedPlace, err := cqrs.WrapCommand(
		placeHandler,
		cqrs.CausationMiddleware(),
		loggingCommandMiddleware(logger),
	)
	if err != nil {
		return nil, nil, err
	}

	getHandler := cqrs.QueryFunc[GetOrder, OrderView](func(ctx context.Context, q GetOrder) (OrderView, error) {
		return OrderView{OrderID: q.OrderID, Status: "PLACED"}, nil
	})

	wrappedGet, err := cqrs.WrapQuery(getHandler)
	if err != nil {
		return nil, nil, err
	}

	return wrappedPlace, wrappedGet, nil
}
```

When you have many command or query types, polymorphic dispatch lists, or want one centralized place to attach cross-cutting middlewares and error policies, `CommandBus` and `QueryBus` provide runtime routing by concrete message type on top of the exact same handler and middleware abstractions.

## Composable middlewares

Middlewares are intentionally non-generic so cross-cutting concerns—logging, tracing, metrics, retries, or database transactions—can be written once without repeating `[C]`, `[Q]`, or `[R]` type parameters:

- `CommandMiddleware` wraps `UntypedCommandFunc` (`func(ctx context.Context, cmd any) error`).
- `QueryMiddleware` wraps `UntypedQueryFunc` (`func(ctx context.Context, query any) (any, error)`).

Both `ChainCommandMiddleware` and `ChainQueryMiddleware` ignore `nil` middlewares and execute in **outermost-first** order: the first middleware passed to `WrapCommand`, `WrapQuery`, or bus options runs before subsequent ones and observes their returned errors on the way back out.

```go
func loggingCommandMiddleware(logger *slog.Logger) cqrs.CommandMiddleware {
	return func(next cqrs.UntypedCommandFunc) cqrs.UntypedCommandFunc {
		return func(ctx context.Context, cmd any) error {
			err := next(ctx, cmd)
			if err != nil {
				logger.ErrorContext(ctx, "command failed",
					"command_id", cqrs.CommandIDFromContext(ctx),
					"trace_id", cqrs.TraceIDFromContext(ctx),
					"err", err,
				)
			}
			return err
		}
	}
}
```

Both `WrapCommand` and `WrapQuery` guard against nil or typed-nil handlers (`ErrNilHandler`) and normalize a `nil` context to `context.Background()`.

## CommandBus and boundary error policies

`CommandBus` maps each concrete Go command type to a single handler. Register handlers during initialization using `RegisterCommand` or `MustRegisterCommand`, then dispatch commands with `bus.Dispatch(ctx, cmd)`.

```go
bus, err := cqrs.NewCommandBus(
	cqrs.WithCommandMiddleware(
		cqrs.CausationMiddleware(),
	),
	cqrs.WithCommandErrorClassifier(func(err error) error {
		if errors.Is(err, errInsufficientFunds) {
			return cqrs.Reject(err)
		}
		return err
	}),
	cqrs.WithOnRejected(func(ctx context.Context, cmd any, err error) {
		slog.WarnContext(ctx, "command rejected by domain",
			"command_id", cqrs.CommandIDFromContext(ctx),
			"err", err,
		)
	}),
)
if err != nil {
	return err
}

if err := cqrs.RegisterCommand(bus, placeOrderHandler, perRouteMiddleware); err != nil {
	return err
}
```

Bus-wide middlewares (`WithCommandMiddleware`) wrap route-specific middlewares supplied to `RegisterCommand`. Lookups match the exact dynamic type passed to `Dispatch`, so registering `PlaceOrder` as a value means callers must dispatch `PlaceOrder{...}` rather than `&PlaceOrder{...}` (which would fail with `ErrNoHandler`). Interface command types are rejected at registration time with `ErrInvalidMessageType`.

### Pre-dispatch vs. post-lookup error policies

`CommandBus.Dispatch` draws a strict line between wiring/caller mistakes before dispatch and runtime errors returned by a handler or its middleware chain:

- **Pre-dispatch failures always surface untouched** regardless of error policy: `ErrNilBus`, `ErrNilCommand` (including typed-nil pointers), `ErrNoHandler`, and an already-canceled `ctx.Err()`.
- **Post-lookup failures** obey one of three explicit boundary policies:

| Option | Behavior on post-lookup error | Typical use |
| :--- | :--- | :--- |
| *(default)* `MaskRejections` | Errors marked via `cqrs.Reject` (or mapped by `WithCommandErrorClassifier`) trigger optional `WithOnRejected` and return `nil` from `Dispatch`; unmarked errors surface untouched | Public API / UI edges where domain rejections are expected outcomes and infrastructure faults must surface |
| `WithStrictOpaqueDispatch(onError)` | Forwards every post-lookup error to `onError` and returns `nil` from `Dispatch` (requires non-nil `onError`, returning `ErrNilErrorSink` otherwise) | Fire-and-forget edges or strict boundary handlers where callers must not branch on internal failures |
| `WithPassthroughErrors()` | Returns every handler and middleware error untouched | Internal service calls, tests, or worker consumers that inspect raw domain sentinels |

### Marking domain rejections with `Reject`

`cqrs.Reject(err)` tags an error as a deterministic business rejection while preserving its unwrap chain for `errors.Is` and `errors.As`. Calling `Reject(nil)` returns `nil`, calling `Reject` on an already-rejected error is idempotent, and `cqrs.IsRejected(err)` searches wrapped and `errors.Join`ed error trees safely.

If you prefer keeping `cqrs` imports out of your pure domain package, return plain domain sentinel errors from your handlers and translate them centrally with `WithCommandErrorClassifier`.

## QueryBus and result type safety

`QueryBus` mirrors `CommandBus` for read operations without error masking: handler errors pass straight back to the caller.

```go
queryBus, err := cqrs.NewQueryBus(
	cqrs.WithQueryMiddleware(readTimeoutMiddleware),
)
if err != nil {
	return err
}

cqrs.MustRegisterQuery(queryBus, cqrs.QueryFunc[GetOrder, OrderView](
	func(ctx context.Context, q GetOrder) (OrderView, error) {
		return repo.FindOrder(ctx, q.OrderID)
	},
))

// Specify only R (OrderView); Q (GetOrder) is inferred automatically.
view, err := cqrs.Query[OrderView](ctx, queryBus, GetOrder{OrderID: "ord-1"})
```

Notice that `cqrs.Query[R, Q]` places the result type `R` first so callers only write `cqrs.Query[OrderView](ctx, bus, GetOrder{...})`. At dispatch time `QueryBus` verifies that the requested `R` matches the type registered with `RegisterQuery`, returning `ErrInvalidResultType` if the caller or a middleware returns an incompatible type. Returning a typed `nil` pointer or interface value (`return nil, nil`) is handled safely as a zero `R` with no error.

## Context causation propagation

Event-driven workflows often chain commands—for example, placing an order triggers a follow-up confirmation or reservation command. `CausationMiddleware` tracks that tree through `context.Context` using three identifiers in `cqrs.Causation`:

- `TraceID` stays constant across the entire logical workflow. If incoming `ctx` already carries a `TraceID` (e.g., via `cqrs.WithTraceID`), it is preserved; otherwise a new ID is generated.
- `CommandID` is freshly minted for each command execution.
- `CausationID` records the parent `CommandID` present on incoming `ctx`, if any (otherwise any existing `CausationID` on `ctx` is kept).

```go
seedCtx := cqrs.WithTraceID(context.Background(), "req-trace-9a1f")

// Inside PlaceOrder handler:
// c, _ := cqrs.CausationFromContext(ctx)
// -> c.TraceID == "req-trace-9a1f", c.CommandID == "<cmd-1>", c.CausationID == ""
//
// If PlaceOrder (or an observer holding PlaceOrder's context) dispatches ConfirmOrder:
// -> c.TraceID == "req-trace-9a1f", c.CommandID == "<cmd-2>", c.CausationID == "<cmd-1>"
```

When using `eventsalsa/store`, map `c.TraceID` to `TraceID` and `c.CommandID` (falling back to `c.CausationID` if `CommandID` is empty) to `CausationID` on each `store.Event` before appending.

By default `CausationMiddleware` generates UUIDv4 strings via `github.com/google/uuid`. Supply `cqrs.WithIDGenerator(fn)` to use custom IDs (such as UUIDv7 or deterministic IDs in tests); if `fn` returns an empty string, `CausationMiddleware` falls back to `uuid.NewString()`.

## Retries and middleware ordering

`RetryCommandMiddleware` wraps a command handler with full-jitter exponential backoff retries whenever `RetryConfig.RetryIf(err)` returns `true`:

```go
handler, err := cqrs.WrapCommand(
	orderHandler,
	cqrs.CausationMiddleware(),
	cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  250 * time.Millisecond,
		RetryIf: func(err error) bool {
			return errors.Is(err, errOptimisticConflict)
		},
	}),
	txMiddleware,
)
```

`RetryConfig` fails safe: if `RetryIf` is `nil` or `MaxAttempts <= 1`, the middleware executes the handler at most once. When retries are enabled, `MaxBackoff` defaults to `250ms` if unset (`<= 0`), and `BaseBackoff` defaults to `min(10ms, MaxBackoff)`. Between attempts, backoff delays are drawn uniformly from `[0, min(BaseBackoff * 2^(attempt-1), MaxBackoff)]`, and context cancellation aborts both before attempting and during backoff sleeps.

:::tip
Always mount `CausationMiddleware` **outside** `RetryCommandMiddleware`, and mount database transaction middlewares **inside** `RetryCommandMiddleware`:

1. `CausationMiddleware` runs once so all retry attempts share the same `CommandID` and `TraceID`.
2. Each retry runs inside a fresh transaction (`txMiddleware`) after the previous failed attempt rolled back.
:::

## Testing domain logic with `cqrs/spectest`

Event-sourced aggregates separate pure state folding (`initial` + `evolve`) and command validation (`decide`) from database persistence. `eventsalsa.dev/cqrs/spectest` lets you test those pure domain functions in sub-millisecond in-memory `Given / When / Then` specifications with zero database containers.

First, define your pure state, events, `initial`, `evolve`, and `decide` functions:

```go
package order_test

import (
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/spectest"
)

var (
	errAlreadyPlaced = errors.New("order already placed")
	errNotPlaced     = errors.New("order not placed")
)

type OrderPlaced struct{ CustomerID string }
type OrderConfirmed struct{}

type PlaceOrder struct{ CustomerID string }
type ConfirmOrder struct{}

type OrderState struct {
	ID         string
	CustomerID string
	Placed     bool
	Confirmed  bool
}

func initialOrder(id string) OrderState {
	return OrderState{ID: id}
}

func evolveOrder(s OrderState, ev any) OrderState {
	switch e := ev.(type) {
	case OrderPlaced:
		s.CustomerID = e.CustomerID
		s.Placed = true
	case OrderConfirmed:
		s.Confirmed = true
	}
	return s
}

func decidePlaceOrder(s OrderState, cmd PlaceOrder) ([]any, error) {
	if s.Placed {
		return nil, errAlreadyPlaced
	}
	return []any{OrderPlaced{CustomerID: cmd.CustomerID}}, nil
}

func decideConfirmOrder(s OrderState, _ ConfirmOrder) ([]any, error) {
	if !s.Placed {
		return nil, errNotPlaced
	}
	if s.Confirmed {
		return nil, nil // idempotent no-op
	}
	return []any{OrderConfirmed{}}, nil
}
```

Next, wire `spectest.For(t, initialOrder, evolveOrder)` inside a standard Go test. Because `Spec.When` and `GivenSpec.When` are Go 1.27 generic methods, each call infers command type `C` directly from the argument and `decide` function without extra type annotations:

```go
func TestOrderSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, initialOrder, evolveOrder)

	// Empty stream via Spec.When: verify emitted events and post-command state.
	spec.When("ord-1", PlaceOrder{CustomerID: "cust-42"}, decidePlaceOrder).
		Then(OrderPlaced{CustomerID: "cust-42"}).
		ThenState(func(s OrderState) {
			if !s.Placed || s.CustomerID != "cust-42" {
				t.Fatalf("unexpected state: %+v", s)
			}
		})

	// Pre-populated stream via Given: verify idempotent no-op emits zero events.
	spec.Given(
		OrderPlaced{CustomerID: "cust-42"},
		OrderConfirmed{},
	).When("ord-1", ConfirmOrder{}, decideConfirmOrder).
		ThenNoEvents()

	// Domain rejection: verify error matching via errors.Is.
	spec.Given().
		When("ord-1", ConfirmOrder{}, decideConfirmOrder).
		ThenErrorIs(errNotPlaced)
}
```

Available assertions on `Outcome[S, E]`:

- `Then(want ...E)` — asserts no error and exact emitted events (`reflect.DeepEqual`, or zero events if `want` is empty).
- `ThenNoEvents()` — asserts no error and zero emitted events.
- `ThenState(check func(state S))` — asserts no error and invokes `check` with the post-command state folded from `Given` + newly emitted events.
- `ThenError()` — asserts a non-nil error was returned.
- `ThenErrorIs(target error)` — asserts the returned error matches `target` via `errors.Is`.
- `ThenErrorCheck(check func(err error))` — asserts a non-nil error and passes it to `check` for custom `errors.As` or field assertions.

## Sentinel errors reference

| Sentinel | Returned when |
| :--- | :--- |
| `ErrNilBus` | A nil `*CommandBus` or `*QueryBus` is passed to registration or dispatch |
| `ErrNilCommand` | A nil or typed-nil command is passed to `CommandBus.Dispatch` |
| `ErrNilQuery` | A nil or typed-nil query is passed to `cqrs.Query` |
| `ErrNilHandler` | A nil or typed-nil handler is passed to `WrapCommand`, `WrapQuery`, `RegisterCommand`, or `RegisterQuery` |
| `ErrNilErrorSink` | `WithStrictOpaqueDispatch(nil)` is supplied to `NewCommandBus` |
| `ErrInvalidMessageType` | `RegisterCommand` or `RegisterQuery` is parameterized with an interface type rather than a concrete type |
| `ErrDuplicateHandler` | A handler is already registered for the concrete command or query type |
| `ErrNoHandler` | No handler is registered for the dispatched command or query's exact dynamic type |
| `ErrInvalidResultType` | `cqrs.Query[R]` requests a different `R` than registered, or a `QueryMiddleware` returns an incompatible type |
