# eventsalsa/cqrs

[![CI](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/eventsalsa.dev/cqrs)](https://goreportcard.com/report/eventsalsa.dev/cqrs)
[![GoDoc](https://pkg.go.dev/badge/eventsalsa.dev/cqrs.svg)](https://pkg.go.dev/eventsalsa.dev/cqrs)

`eventsalsa.dev/cqrs` provides lightweight, transport- and database-free building blocks for Command and Query Responsibility Segregation (CQRS) in Go 1.27+.

It separates write-side domain commands (`eventsalsa.dev/cqrs/command`) from lightweight UI and reporting queries (`eventsalsa.dev/cqrs/query`), propagates observability context (`eventsalsa.dev/cqrs`), and offers in-memory domain behavior testing (`eventsalsa.dev/cqrs/spectest`) while relying only on the Go standard library and `github.com/google/uuid`. It works cleanly in standard CRUD applications as well as alongside `eventsalsa/store` and `eventsalsa/projector`.

## Features

- **Zero database or broker coupling** — no `pgx`, `store`, `projector`, HTTP router, or message-broker dependencies; works with or without Event Sourcing
- **Clean command/query split** — separate `command` (`error`) and `query` (`(R, error)`) packages with `Bus.Dispatch` and `query.Execute[R]` entrypoints
- **Direct handlers or central buses** — decorate individual `command.Handler[C]` or `query.Handler[Q, R]` values via `Wrap`, or register handlers centrally on `command.Bus` and `query.Bus`
- **Custom and built-in middlewares** — write logging, timeouts, metrics, or transaction boundaries via `command.Middleware` / `query.Middleware`, plus built-in `ObservabilityMiddleware` and `command.RetryMiddleware`
- **Failure boundary control** — distinguish expected domain rejections (`command.Reject`, `WithErrorClassifier`) from infrastructure faults with explicit dispatch policies (`maskRejection` default, `WithStrictOpaqueDispatch`, `WithPassthroughErrors`)
- **Observability** — `cqrs.Observability` propagates `TraceID`, `CommandID`, and `CausationID` across nested commands and reads
- **Domain testing** — `eventsalsa.dev/cqrs/spectest` runs sub-millisecond `Given / When / Then` specs over pure `initial`, `evolve`, and `decide` functions

## Package Overview

| Package | Role |
| :--- | :--- |
| `eventsalsa.dev/cqrs` | Shared types, context helpers, and common sentinel errors |
| `eventsalsa.dev/cqrs/command` | Write-side command handling, routing, middlewares, and failure boundary policies |
| `eventsalsa.dev/cqrs/query` | Read-side query handling, typed execution, routing, and middlewares |
| `eventsalsa.dev/cqrs/spectest` | In-memory `Given / When / Then` domain testing |

## Quick Start

### 1. Install

```bash
go get eventsalsa.dev/cqrs
```

Requires Go `1.27.4+`.

### 2. Write side (`cqrs/command`)

Handlers accept a strongly typed command `C` and return `error`. Attach bus-wide middlewares (observability, retries, transactions) once, classify domain rejections cleanly, and call `bus.Dispatch`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"eventsalsa.dev/cqrs"
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
		command.WithOnRejected(func(ctx context.Context, rejected any, err error) {
			slog.WarnContext(ctx, "command rejected",
				"trace_id", cqrs.TraceIDFromContext(ctx),
				"command", fmt.Sprintf("%T", rejected),
				"err", err,
			)
		}),
	)
	if err != nil {
		return err
	}

	command.MustRegister(bus, command.Func[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		// Validate rules and persist write-side state using ctx.
		return nil
	}))

	// Masked rejections return nil; unhandled infrastructure faults return non-nil errors.
	return bus.Dispatch(ctx, PlaceOrder{OrderID: "ord-1", CustomerID: "cust-42"})
}
```

### 3. Read side (`cqrs/query`)

Keep queries lightweight and focused on UI, API, and reporting reads without invoking them inside write-side business logic:

```go
package main

import (
	"context"

	"eventsalsa.dev/cqrs/query"
)

type GetOrder struct{ OrderID string }
type OrderView struct{ OrderID, Status string }

func runQueries(ctx context.Context, viewRepo OrderViewReader) (OrderView, error) {
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

### 4. Testing (`cqrs/spectest`)

Validate pure state evolution and command decision functions in memory before wiring persistence:

```go
spec := spectest.For(t, initialOrder, evolveOrder)

spec.Given(OrderPlaced{CustomerID: "cust-42"}).
	When("ord-1", ConfirmOrder{}, decideConfirmOrder).
	Then(OrderConfirmed{})
```

## Examples

Runnable samples live in the standalone [`examples/`](./examples/) sub-module:

- **[`examples/without-event-sourcing/style-1`](./examples/without-event-sourcing/style-1/)** — CQRS without Event Sourcing: exported Go `commands`/`queries` structs, domain invariants, and `internal/application` write handlers
- **[`examples/without-event-sourcing/style-2`](./examples/without-event-sourcing/style-2/)** — CQRS without Event Sourcing: Protobuf `commands`/`queries` contracts, `command.Func` ACL adapters in `internal/infra/acl`, internal `internal/application` commands/handlers, and `internal/domain` entity invariants
- **[`examples/event-sourcing`](./examples/event-sourcing/)** — CQRS with Event Sourcing using `eventsalsa/store`, inline projection, and `cqrs/spectest` domain specs

## Package Layout

```text
cqrs/
├── doc.go                  # Module overview and package guide
├── errors.go               # Shared bus/registration sentinels
├── observability.go        # Shared Observability context (TraceID, CommandID, CausationID)
├── command/                # Write-side handlers, bus, middlewares, rejection boundaries
├── query/                  # Read-side handlers, bus, typed Execute, middlewares
├── spectest/               # Given/When/Then pure domain test harness
├── internal/cqrsutil/      # Shared reflection guards, middleware chaining, ID helper
├── examples/               # Standalone unpublished runnable examples
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
