---
title: CQRS
description: Separate write commands, read queries, observability propagation, retries, and fast domain specs with eventsalsa/cqrs.
---

`eventsalsa/cqrs` provides lightweight, transport- and database-free building blocks for Command and Query Responsibility Segregation (CQRS) in Go.

In an event-sourced application, write operations validate business rules and persist facts, while read operations return projections shaped for callers. `eventsalsa/cqrs` stays strictly at the application boundary between transports and domain logic: it gives you typed command and query contracts, non-generic middleware pipelines, optional buses with explicit failure semantics, shared causal observability context, retries, and a fast `Given / When / Then` test harness (`cqrs/spectest`).

It has zero dependencies on PostgreSQL, `pgx`, `eventsalsa/store`, `eventsalsa/projector`, or message brokers. Only the Go standard library and `github.com/google/uuid` are required, so you can pair it with the rest of eventsalsa or use it inside any Go service.

> `eventsalsa.dev/cqrs` is currently in the `0.0.*` pre-release series while its ergonomics settle before `v0.1.0`.

## Package layout

The module splits write-side concerns, read-side concerns, shared observability, and domain specs into distinct packages so signatures stay minimal:

| Package | Responsibility |
| --- | --- |
| `eventsalsa.dev/cqrs` | Shared `Observability` context (`TraceID`, `CommandID`, `CausationID`) and common bus/registration sentinel errors |
| `eventsalsa.dev/cqrs/command` | Write-side `Handler[C]`, `Middleware`, `Wrap`, `Bus.Dispatch`, `Reject`/`IsRejected`, `ObservabilityMiddleware`, and `RetryMiddleware` |
| `eventsalsa.dev/cqrs/query` | Read-side `Handler[Q, R]`, `Middleware`, `Wrap`, `Bus`, `Execute[R, Q]`, and `ObservabilityMiddleware` |
| `eventsalsa.dev/cqrs/spectest` | Sub-millisecond `Given / When / Then` BDD specs over pure `initial`, `evolve`, and `decide` functions |

## Install the module

Add `eventsalsa.dev/cqrs` to your module (`Go 1.27.4+` required):

```bash
go get eventsalsa.dev/cqrs
```

No database migrations or background workers are needed for this package.

## Commands vs. queries

Commands and queries solve fundamentally different jobs, so `eventsalsa/cqrs` isolates them into separate packages rather than squeezing them into one interface:

- **Commands (`cqrs/command`) return only `error`.** Write pipelines focus on authorization, transactional boundaries, conflict retries, causation tracking, and distinguishing domain rejections from unexpected infrastructure failures.
- **Queries (`cqrs/query`) return `(R, error)`.** Read pipelines focus on timeouts, read-only database transactions, caching, and returning typed projections without write-side retry or rejection machinery.

If your HTTP handlers or workers prefer calling explicit service objects directly, you do not have to adopt a central bus. Decorate individual handlers once at startup with `command.Wrap` or `query.Wrap`, and call `.Handle(ctx, msg)` straight from your transport layer:

```go
package main

import (
	"context"
	"log/slog"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/query"
)

type PlaceOrder struct {
	OrderID    string
	CustomerID string
}

type GetOrder struct{ OrderID string }
type OrderView struct{ OrderID, Status string }

func buildEndpoints(logger *slog.Logger) (command.Handler[PlaceOrder], query.Handler[GetOrder, OrderView], error) {
	placeOrder := command.Func[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		// Validate intent, load stream state, append events.
		return nil
	})

	wrappedPlace, err := command.Wrap(
		placeOrder,
		command.ObservabilityMiddleware(),
		logCommandFailures(logger),
	)
	if err != nil {
		return nil, nil, err
	}

	getOrder := query.Func[GetOrder, OrderView](func(ctx context.Context, q GetOrder) (OrderView, error) {
		return OrderView{OrderID: q.OrderID, Status: "PLACED"}, nil
	})

	wrappedGet, err := query.Wrap(getOrder, query.ObservabilityMiddleware())
	if err != nil {
		return nil, nil, err
	}

	return wrappedPlace, wrappedGet, nil
}
```

Both `command.Wrap` and `query.Wrap` reject `nil` or typed-nil handlers upfront (`cqrs.ErrNilHandler`) and preserve the typed `Handler` interface for callers.

:::caution
Pass a valid, non-nil `context.Context` everywhere. Following Go's standard context contract (and matching `eventsalsa/store`, `projector`, and `encryption`), `eventsalsa/cqrs` never invents `context.Background()` when given `nil` — passing `nil` fails loudly rather than silently dropping cancellation signals or request deadlines.
:::

## Composable middlewares

In Go, attaching `[C]`, `[Q]`, or `[R]` type parameters to every logging, metrics, transaction, or timeout middleware quickly makes wiring unreadable. Instead, middlewares operate on type-erased functions once inside the pipeline:

- `command.Middleware` wraps `command.UntypedFunc` (`func(ctx context.Context, cmd any) error`).
- `query.Middleware` wraps `query.UntypedFunc` (`func(ctx context.Context, q any) (any, error)`).

Here is an example of writing one reusable command logger that works for any command struct without generics:

```go
func logCommandFailures(logger *slog.Logger) command.Middleware {
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) error {
			err := next(ctx, cmd)
			if err != nil {
				logger.ErrorContext(ctx, "command failed",
					"trace_id", cqrs.TraceIDFromContext(ctx),
					"command_id", cqrs.CommandIDFromContext(ctx),
					"err", err,
				)
			}
			return err
		}
	}
}
```

`command.Chain` and `query.Chain` ignore `nil` middlewares cleanly and execute in **outermost-first** order: the first middleware listed runs earliest on the way in and observes errors last on the way back out.

## Routing with `command.Bus` and boundary policies

Once an application grows beyond a handful of handlers—or when workers, schedulers, and HTTP adapters dispatch polymorphic command lists—a central `command.Bus` eliminates per-route wiring repetition. Bind each concrete Go command type with `command.Register` (or `command.MustRegister`) and dispatch values with `bus.Dispatch(ctx, cmd)`.

More importantly, a `command.Bus` gives you a single place to enforce how failures behave at the boundary of your system. Not every error means the same thing to a caller:

- **Pre-dispatch bugs or caller mistakes** (`cqrs.ErrNilBus`, `command.ErrNilCommand`, `cqrs.ErrNoHandler`, or an already-canceled `ctx.Err()`) always return immediately before lookup succeeds or handlers run.
- **Domain rejections** (`"order already canceled"`, `"insufficient balance"`) are deterministic business rules, often handled via HTTP `422` or an internal hook rather than treated like a crashed worker.
- **Infrastructure faults** (database unreachable, network timeout) must bubble up cleanly so retries, alerting, or HTTP `500` handling trigger reliably.

You mark expected business rules either inline with `command.Reject(err)` or centrally inside the bus using `command.WithErrorClassifier` so your domain handlers keep plain sentinel errors:

```go
bus, err := command.NewBus(
	command.WithMiddleware(command.ObservabilityMiddleware()),
	command.WithErrorClassifier(func(err error) error {
		if errors.Is(err, errInsufficientBalance) {
			return command.Reject(err)
		}
		return err
	}),
	command.WithOnRejected(func(ctx context.Context, _ any, err error) {
		slog.WarnContext(ctx, "business rule rejected command",
			"command_id", cqrs.CommandIDFromContext(ctx),
			"err", err,
		)
	}),
)
if err != nil {
	return err
}

if err := command.Register(bus, placeOrderHandler, perRouteTimeout); err != nil {
	return err
}
```

Under the default policy (`maskRejection`), marked rejections trigger any `WithOnRejected` hook and return `nil` from `bus.Dispatch`, while unmarked infrastructure errors surface to the caller untouched. When a boundary calls for different behavior, two explicit alternatives are available:

| Option | Behavior on post-lookup errors | Typical fit |
| --- | --- | --- |
| *(default)* Rejection masking | Marked rejections (`command.Reject` / `WithErrorClassifier`) invoke `WithOnRejected` and return `nil`; unmarked errors return untouched | Public edges or fire-and-forget dispatchers separating domain rejections from infra outages |
| `command.WithStrictOpaqueDispatch(onError)` | Forwards every post-lookup failure to `onError` and always returns `nil` from `Dispatch` (`ErrNilErrorSink` if `onError` is nil) | Strict boundary handlers where callers must never branch on downstream failures |
| `command.WithPassthroughErrors()` | Returns every handler and middleware error untouched | HTTP controllers returning mapped status codes, internal callers, and integration tests |

Lookups match the exact dynamic type passed to `Dispatch`, so registering `PlaceOrder` as a value expects callers to pass `PlaceOrder{...}` rather than `&PlaceOrder{...}`. Registering an interface type fails at startup with `cqrs.ErrInvalidMessageType`.

## Type-safe reads with `query.Bus` and `query.Execute`

`query.Bus` mirrors `command.Bus` for read-only projections without masking errors—any failure returned by a query handler flows straight back to the caller.

Because Go does not support type parameters on struct methods, executing a query uses the package function `query.Execute[R](ctx, bus, q)` with the result type `R` placed first. Go infers `Q` automatically from `q`:

```go
qBus, err := query.NewBus(
	query.WithMiddleware(query.ObservabilityMiddleware()),
)
if err != nil {
	return err
}

query.MustRegister(qBus, query.Func[GetOrder, OrderView](
	func(ctx context.Context, q GetOrder) (OrderView, error) {
		return viewRepo.LoadOrder(ctx, q.OrderID)
	},
))

// R (OrderView) is specified; Q (GetOrder) is inferred.
view, err := query.Execute[OrderView](ctx, qBus, GetOrder{OrderID: "ord-1"})
```

At runtime `query.Execute` checks that the requested `R` matches the type registered for `Q`, guarding against caller mistakes or buggy middlewares that return an unexpected type (`query.ErrInvalidResultType`). Returning a typed `nil` pointer (`return nil, nil`) passes cleanly back as a zero `R` with no error.

## Observability and causal propagation

As soon as workflows chain commands together—or trigger downstream handlers through [`eventsalsa/store`](../store/) and [`eventsalsa/projector`](../projector/)—knowing *which request* started a flow (`TraceID`) and *which specific step* caused the next (`CommandID` -> `CausationID`) turns debugging from guesswork into an exact tree.

The root `cqrs` package stores those three strings in `cqrs.Observability` on `context.Context`:

- `TraceID` stays fixed across the entire logical workflow. Incoming trace IDs attached via `cqrs.WithTraceID` are preserved; otherwise middleware mints one automatically.
- `CommandID` is freshly generated for every command execution so retries and follow-up actions stay distinguishable.
- `CausationID` captures the incoming context's `CommandID` whenever one command dispatches or triggers another (preserving any prior `CausationID` otherwise).

Mounting `command.ObservabilityMiddleware` handles stamping automatically on commands, while `query.ObservabilityMiddleware` ensures a `TraceID` exists on standalone reads without fabricating synthetic `CommandID` values or clearing existing causal context:

```go
seedCtx := cqrs.WithTraceID(context.Background(), "http-req-9a1f")

// Inside PlaceOrder command handler:
// obs, _ := cqrs.ObservabilityFromContext(ctx)
// -> obs.TraceID == "http-req-9a1f", obs.CommandID == "<cmd-1>", obs.CausationID == ""
//
// If PlaceOrder dispatches ConfirmOrder using ctx:
// -> obs.TraceID == "http-req-9a1f", obs.CommandID == "<cmd-2>", obs.CausationID == "<cmd-1>"
```

When saving events with [`eventsalsa/store`](../store/), copy `obs.TraceID` into `store.Event.TraceID` and `obs.CommandID` (or `obs.CausationID` outside a command) into `store.Event.CausationID`. By default IDs use `github.com/google/uuid` (`uuid.NewString`), or you can supply a custom generator via `WithIDGenerator` (for UUIDv7 or deterministic test sequences).

## Retries and middleware layering

Transient failures—such as `store.ErrOptimisticConcurrency` during concurrent stream writes—are expected in event-sourced systems. `command.RetryMiddleware` retries failures matching `RetryConfig.RetryIf` using full-jitter exponential backoff up to `MaxAttempts`:

```go
handler, err := command.Wrap(
	placeOrderHandler,
	command.ObservabilityMiddleware(),
	command.RetryMiddleware(command.RetryConfig{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  250 * time.Millisecond,
		RetryIf: func(err error) bool {
			return errors.Is(err, store.ErrOptimisticConcurrency)
		},
	}),
	txMiddleware,
)
```

If `RetryIf` is `nil` or `MaxAttempts <= 1`, the handler runs once and fails safely. Otherwise delays are drawn uniformly from `[0, min(BaseBackoff * 2^(attempt-1), MaxBackoff)]`, with `MaxBackoff` defaulting to `250ms` and `BaseBackoff` to `min(10ms, MaxBackoff)`. Context cancellation aborts both before starting an attempt and immediately during backoff sleeps.

:::tip
Middleware order matters when combining observability, retries, and transactions:

1. Mount `command.ObservabilityMiddleware` **outside** `command.RetryMiddleware` so retry attempts share a single `CommandID` and `TraceID`.
2. Mount database transaction middlewares **inside** `command.RetryMiddleware` so each retry starts a clean transaction after the previous failed attempt rolled back.
:::

## Fast domain testing with `cqrs/spectest`

Event-sourced domain models are easiest to test when pure state folding (`initial` + `evolve`) and business decisions (`decide`) do not import SQL or test containers. `eventsalsa.dev/cqrs/spectest` gives you an in-memory `Given / When / Then` test harness built around those exact pure functions:

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

Wire `spectest.For(t, initialOrder, evolveOrder)` in a standard Go unit test. Because `Spec.When` and `GivenSpec.When` are Go 1.27 generic methods, each call infers command type `C` directly from your command argument and `decide` function:

```go
func TestOrderSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, initialOrder, evolveOrder)

	spec.When("ord-1", PlaceOrder{CustomerID: "cust-42"}, decidePlaceOrder).
		Then(OrderPlaced{CustomerID: "cust-42"}).
		ThenState(func(s OrderState) {
			if !s.Placed || s.CustomerID != "cust-42" {
				t.Fatalf("unexpected state: %+v", s)
			}
		})

	spec.Given(
		OrderPlaced{CustomerID: "cust-42"},
		OrderConfirmed{},
	).When("ord-1", ConfirmOrder{}, decideConfirmOrder).
		ThenNoEvents()

	spec.Given().
		When("ord-1", ConfirmOrder{}, decideConfirmOrder).
		ThenErrorIs(errNotPlaced)
}
```

Use `Then(...)` for exact emitted events (`reflect.DeepEqual`), `ThenNoEvents()` for idempotent no-ops, `ThenState(...)` for post-command state checks, and `ThenError()` / `ThenErrorIs(...)` / `ThenErrorCheck(...)` for expected business rejections. Because no PostgreSQL instance is started, entire domain suites finish in sub-millisecond time.
