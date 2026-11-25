# eventsalsa/cqrs

[![CI](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/eventsalsa.dev/cqrs)](https://goreportcard.com/report/eventsalsa.dev/cqrs)
[![GoDoc](https://pkg.go.dev/badge/eventsalsa.dev/cqrs.svg)](https://pkg.go.dev/eventsalsa.dev/cqrs)

`eventsalsa.dev/cqrs` is a lightweight, transport- and database-free CQRS module for Go 1.27+.

It separates write-side domain commands (`eventsalsa.dev/cqrs/command`) from lightweight presentation and reporting queries (`eventsalsa.dev/cqrs/query`), carries trace and causal context across handlers (`eventsalsa.dev/cqrs`), and provides sub-millisecond domain behavior specs (`eventsalsa.dev/cqrs/spectest`) while relying only on the Go standard library and `github.com/google/uuid`. It works cleanly in standard relational CRUD apps as well as alongside `eventsalsa/store` and `eventsalsa/projector`.

> **Pre-release status:** `eventsalsa.dev/cqrs` is developed in the `0.0.*` pre-release series while its ergonomics stabilize prior to `v0.1.0`.

## Features

- **Zero database or broker coupling** — no `pgx`, `store`, `projector`, HTTP router, or message-broker dependencies; usable with or without Event Sourcing
- **Clean command/query split** — separate `command` (`error`) and `query` (`(R, error)`) packages with idiomatic `Bus.Dispatch` and `query.Execute[R]` entrypoints
- **Direct handlers or optional buses** — wrap individual `command.Handler[C]` or `query.Handler[Q, R]` values with `Wrap`, or route dynamically through `command.Bus` and `query.Bus`
- **Non-generic middleware pipelines** — write logging, timeouts, metrics, retries, or transactions once over `command.UntypedFunc` / `query.UntypedFunc` without generic boilerplate
- **Deterministic rejection boundaries** — distinguish expected domain rejections (`command.Reject`, `WithErrorClassifier`) from infrastructure faults with explicit dispatch policies (`maskRejection` default, `WithStrictOpaqueDispatch`, `WithPassthroughErrors`)
- **Cross-cutting observability context** — `cqrs.Observability` carries `TraceID`, `CommandID`, and `CausationID` across commands (`command.ObservabilityMiddleware`) and queries (`query.ObservabilityMiddleware`)
- **Jittered exponential retries** — `command.RetryMiddleware` retries explicitly matched failures with full jitter and strict context-cancellation awareness
- **Zero-DB domain specs** — `eventsalsa.dev/cqrs/spectest` tests pure `initial`, `evolve`, and `decide` functions in a fluent `Given / When / Then` Go 1.27 API

## Architecture

```text
Caller (UI / API / Worker / Test)
   │
   ├── Write ──▶ command.Bus.Dispatch (or wrapped command.Handler[C])
   │               └─▶ ObservabilityMiddleware -> RetryMiddleware -> App/Tx -> Write Model
   │                     └─ boundary policy: maskRejection | StrictOpaque | Passthrough
   │
   └── Read  ──▶ query.Execute[R] (or wrapped query.Handler[Q, R])
                   └─▶ ObservabilityMiddleware -> Thin Read Handler (SQL / View DTO) (R, error)
```

Keep queries lightweight, side-effect-free, and focused on UI/API/reporting reads—avoid invoking `query.Bus` inside write-side domain flows, where handlers should load authoritative write state directly.

### Package Overview

| Package | Role |
| :--- | :--- |
| `eventsalsa.dev/cqrs` | Shared `Observability` context (`TraceID`, `CommandID`, `CausationID`) and common bus/registration sentinels |
| `eventsalsa.dev/cqrs/command` | `Handler[C]`, `Middleware`, `Wrap`, `Bus.Dispatch`, `Reject`/`IsRejected`, `ObservabilityMiddleware`, `RetryMiddleware` |
| `eventsalsa.dev/cqrs/query` | `Handler[Q, R]`, `Middleware`, `Wrap`, `Bus`, `Execute[R, Q]`, `ObservabilityMiddleware` |
| `eventsalsa.dev/cqrs/spectest` | In-memory `Given / When / Then` BDD specification harness (`Spec`, `GivenSpec`, `Outcome`) |

## Quick Start

### 1. Install

```bash
go get eventsalsa.dev/cqrs
```

Requires Go `1.27.4+`.

### 2. Write Commands (`cqrs/command`)

Handlers accept a strongly typed command `C` and return `error`. Attach bus-wide middlewares (observability, retries, transactions) once, classify domain rejections cleanly, and call `bus.Dispatch`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"eventsalsa.dev/cqrs/command"
)

type PlaceOrder struct {
	OrderID    string
	CustomerID string
}

var (
	errCustomerBlocked   = errors.New("customer is blocked")
	errTransientConflict = errors.New("optimistic concurrency conflict")
)

func runCommands(ctx context.Context) error {
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: 10 * time.Millisecond,
				MaxBackoff:  100 * time.Millisecond,
				RetryIf: func(err error) bool {
					return errors.Is(err, errTransientConflict)
				},
			}),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errCustomerBlocked) {
				return command.Reject(err)
			}
			return err
		}),
		command.WithOnRejected(func(ctx context.Context, _ any, err error) {
			slog.WarnContext(ctx, "command rejected", "err", err)
		}),
	)
	if err != nil {
		return err
	}

	command.MustRegister(bus, command.Func[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		// Execute domain decision + persistence using ctx.
		return nil
	}))

	// Masked rejections return nil; unhandled infrastructure faults return non-nil errors.
	return bus.Dispatch(ctx, PlaceOrder{OrderID: "ord-1", CustomerID: "cust-42"})
}
```

### 3. Read Queries (`cqrs/query`)

Queries separate read paths cleanly with `(R, error)` handlers. Put only the result type `R` on `query.Execute[R]` — query type `Q` is inferred from the argument:

```go
package main

import (
	"context"

	"eventsalsa.dev/cqrs/query"
)

type GetOrder struct{ OrderID string }
type OrderView struct{ OrderID, Status string }

func runQueries(ctx context.Context) (OrderView, error) {
	qBus, err := query.NewBus(
		query.WithMiddleware(query.ObservabilityMiddleware()),
	)
	if err != nil {
		return OrderView{}, err
	}

	query.MustRegister(qBus, query.Func[GetOrder, OrderView](
		func(ctx context.Context, q GetOrder) (OrderView, error) {
			return viewRepo.LoadOrder(ctx, q.OrderID)
		},
	))

	return query.Execute[OrderView](ctx, qBus, GetOrder{OrderID: "ord-1"})
}
```

### 4. Test Domain Rules Without a Database (`cqrs/spectest`)

Validate pure state evolution and command decision functions in memory before wiring persistence:

```go
spec := spectest.For(t, initialOrder, evolveOrder)

spec.Given(OrderPlaced{CustomerID: "cust-42"}).
	When("ord-1", ConfirmOrder{}, decideConfirmOrder).
	Then(OrderConfirmed{})
```

## Examples

Runnable samples live in the standalone [`examples/`](./examples/) sub-module:

- **[`examples/simple`](./examples/simple/)** — single-database CQRS without Event Sourcing (zero external dependencies)
- **[`examples/event-sourcing`](./examples/event-sourcing/)** — CQRS combined with `eventsalsa/store` Event Sourcing, inline projection, and `cqrs/spectest` domain specs

## Design Choices

- **Asymmetric `command` and `query` packages**: Command pipelines return `error` and deal with retries, rejections, and causation; query pipelines return `(R, error)` with runtime result type verification. Separating the packages removes naming clutter and lets middleware signatures stay idiomatic.
- **Direct handlers vs. buses**: Not every handler needs dynamic runtime dispatch. `command.Wrap` and `query.Wrap` attach identical `Middleware` chains directly around typed handlers without registering on a `Bus`.
- **Explicit middleware ordering**: Mount `command.ObservabilityMiddleware` **outside** `command.RetryMiddleware` so retry attempts keep one `CommandID` and `TraceID`, and mount transaction middlewares **inside** `command.RetryMiddleware` so each retry runs in a clean transaction after rollback.

## Package Layout

```text
cqrs/
├── doc.go                  # Module overview and package guide
├── errors.go               # Shared bus/registration sentinels
├── observability.go        # Shared Observability context (TraceID, CommandID, CausationID)
├── command/                # Handler[C], Bus.Dispatch, Reject, ObservabilityMiddleware, RetryMiddleware
├── query/                  # Handler[Q, R], Bus, Execute[R, Q], ObservabilityMiddleware
├── spectest/               # Given/When/Then pure domain test harness
├── internal/cqrsutil/      # Shared reflection guards, middleware chaining, ID helper
├── examples/               # Standalone unpublished runnable examples (simple, event-sourcing)
└── integration_test/       # In-process end-to-end CQRS pipeline integration tests
```

## Development

```bash
# Run vet, golangci-lint, race-enabled unit tests, integration tests, gosec, and govulncheck
make check

# Run unit tests directly
go test -race -count=1 ./...

# Run integration tests
go test -race -count=1 -tags=integration ./integration_test/...
```
