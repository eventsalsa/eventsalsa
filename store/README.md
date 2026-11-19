# eventsalsa/store

[![CI](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/eventsalsa/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/eventsalsa.dev/store)](https://goreportcard.com/report/eventsalsa.dev/store)
[![GoDoc](https://pkg.go.dev/badge/eventsalsa.dev/store.svg)](https://pkg.go.dev/eventsalsa.dev/store)

A minimal, production-ready event store for Go.

## Features

- **PostgreSQL-backed event store** — append-only, immutable event log with BIGSERIAL global positions
- **Optimistic concurrency control** — via expected versions enforced at the database level via `stream_heads`
- **Optional range partitioning** — native PostgreSQL declarative RANGE partitioning & `pg_partman` integration
- **Stream reads** — load a full or partial event history with optional version ranges
- **Sequential event reading** — read events by global position for building consumers and projections
- **Transaction-first design** — all operations accept `pgx.Tx`; you control transaction boundaries
- **SQL migration generator** — `cmd/migrate-gen` generates a ready-to-apply `.sql` file
- **Event mapping code generator** — `cmd/eventmap-gen` generates type-safe domain event mappings


## Quick Start

### 1. Install

```bash
go get eventsalsa.dev/store
```

Choose your PostgreSQL driver:

```bash
go get github.com/jackc/pgx/v5
```

### 2. Generate Migrations

```bash
go run eventsalsa.dev/store/cmd/migrate-gen -output migrations
```

Apply the generated file with your preferred migration tool:

```bash
psql -h localhost -U postgres -d mydb -f migrations/*_init_event_sourcing.sql
```

### 3. Append and Read Events

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    "github.com/google/uuid"
    "github.com/jackc/pgx/v5/pgxpool"

    "eventsalsa.dev/store"
    "eventsalsa.dev/store/postgres"
)

func main() {
    ctx := context.Background()
    db, err := pgxpool.New(ctx, "host=localhost user=postgres dbname=mydb sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    s := postgres.NewStore(postgres.DefaultStoreConfig())

    // Append events to a new stream
    userID := uuid.New().String()
    payload, _ := json.Marshal(map[string]string{"email": "alice@example.com", "name": "Alice"})

    tx, err := db.Begin(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer tx.Rollback(ctx) //nolint:errcheck

    result, err := s.Append(ctx, tx, store.NoStream(), []store.Event{
        {
            StreamType:   "User",
            StreamID:     userID,
            EventID:      uuid.New(),
            EventType:    "UserCreated",
            EventVersion: 1,
            Payload:      payload,
            Metadata:     []byte(`{}`),
            CreatedAt:    time.Now(),
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := tx.Commit(ctx); err != nil {
        log.Fatal(err)
    }

    log.Printf("appended at positions %v, stream now at version %d",
        result.GlobalPositions, result.ToVersion())

    // Read the stream
    tx2, _ := db.Begin(ctx)
    defer tx2.Rollback(ctx) //nolint:errcheck

    stream, err := s.ReadStream(ctx, tx2, "User", userID, nil, nil)
    if err != nil {
        log.Fatal(err)
    }
    tx2.Commit(ctx) //nolint:errcheck

    log.Printf("stream: %d events, current version %d", stream.Len(), stream.Version())
    for _, e := range stream.Events {
        log.Printf("  v%d  %s  pos=%d", e.StreamVersion, e.EventType, e.GlobalPosition)
    }
}
```

## Core Concepts

### Events & Streams

`store.Event` is an immutable value object that you construct before persisting. The store assigns `StreamVersion` and `GlobalPosition` during `Append`.

```go
event := store.Event{
    StreamType:    "Order",         // logical category of the stream
    StreamID:      orderID,         // string identifier — UUID, email, slug, etc.
    EventID:       uuid.New(),      // idempotency key for the event itself
    EventType:     "OrderPlaced",   // discriminator used by consumers
    EventVersion:  1,               // schema version of the payload
    Payload:       payload,         // serialized domain data (JSON, proto, etc.)
    Metadata:      metadata,        // cross-cutting concerns (request ID, actor, etc.)
    CreatedAt:     time.Now(),
    // optional tracing fields:
    TraceID:       store.NullString{String: traceID, Valid: true},
    CorrelationID: store.NullString{String: corrID, Valid: true},
    CausationID:   store.NullString{String: causID, Valid: true},
}
```

`store.PersistedEvent` is what you read back. It adds `GlobalPosition`, `TransactionID` (`xid8` PostgreSQL transaction ID), and `StreamVersion`.

`store.Stream` wraps the full ordered history of a single stream along with helper methods:

```go
stream.Version()  // current stream version (0 if empty)
stream.IsEmpty()  // true if no events were found
stream.Len()      // number of events in the stream
```

`store.AppendResult` describes the outcome of a write:

```go
result.ToVersion()       // stream version after the append
result.FromVersion()     // stream version before the append
result.GlobalPositions   // global positions assigned to each event
result.Events            // persisted events with all fields populated
```

### Expected Versions

Expected versions are the mechanism for optimistic concurrency. You declare the state you expect the stream to be in before writing.

| Constructor | When to use |
|---|---|
| `store.NoStream()` | Creating a new stream — fails if it already exists |
| `store.Exact(n)` | Updating an existing stream at a known version — fails on conflict |
| `store.Any()` | Unconditional write — skips version validation entirely |

Conflicts return `store.ErrOptimisticConcurrency`. The database unique constraint on `(stream_type, stream_id, stream_version)` acts as a final safety net even if two transactions pass the application-level check simultaneously.

```go
// Create — must not already exist
result, err := s.Append(ctx, tx, store.NoStream(), events)

// Update at a known version
result, err := s.Append(ctx, tx, store.Exact(stream.Version()), events)

// Unconditional
result, err := s.Append(ctx, tx, store.Any(), events)

if errors.Is(err, store.ErrOptimisticConcurrency) {
    // reload, reapply command, retry
}
```

### Stream Reads

`ReadStream` returns the ordered event history for a single stream instance. Both version bounds are optional and inclusive.

```go
// Full history
stream, err := s.ReadStream(ctx, tx, "User", userID, nil, nil)

// From a specific version onwards (e.g., to skip already-processed events)
from := int64(5)
stream, err = s.ReadStream(ctx, tx, "User", userID, &from, nil)

// A version window
from, to := int64(5), int64(10)
stream, err = s.ReadStream(ctx, tx, "User", userID, &from, &to)
```

### Sequential Reads

`ReadEvents` reads committed events from the global log behind PostgreSQL's active transaction visibility
horizon (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) in `(transaction_id, global_position)`
order, using a composite `store.Cursor` (`TransactionID` + `GlobalPosition`).

```go
// Read up to 500 settled events from the beginning of the global log
events, err := s.ReadEvents(ctx, tx, store.Cursor{}, 500)

// Continue from the last processed event cursor
last := events[len(events)-1]
lastCursor := store.Cursor{
    TransactionID:  last.TransactionID,
    GlobalPosition: last.GlobalPosition,
}
events, err = s.ReadEvents(ctx, tx, lastCursor, 500)
```

`GetHead` returns the highest `store.Cursor` (`TransactionID`, `GlobalPosition`) currently visible
behind `pg_snapshot_xmin(pg_current_snapshot())` — useful for lightweight wakeup or polling checks
without fetching full batches.

```go
head, err := s.GetHead(ctx, tx)
```

> **Do not tail by raw `global_position`:** If you query the `events` SQL table directly or track
> checkpoints using `global_position` alone (`WHERE global_position > $1`), concurrent writers can
> permanently skip events. PostgreSQL `BIGSERIAL` sequences guarantee uniqueness, not commit order
> — an in-flight transaction holding a lower `global_position` can commit after a higher one. Always
> tail the global log using `store.Cursor` behind `pg_snapshot_xmin` via `ReadEvents` / `GetHead`
> (or `eventsalsa.dev/projector`, which manages checkpoints automatically).

Scoped async filtering is intentionally a runtime concern rather than a store read primitive.
If a consumer only acts on specific stream or event types, advance checkpoints using the unscoped
`store.Cursor` returned for each read window and filter matching events inside the handler (`eventsalsa.dev/projector` does this out of the box).

## PostgreSQL Implementation

### Configuration

`postgres.NewStore` accepts a `*postgres.StoreConfig` built with functional options:

```go
s := postgres.NewStore(postgres.NewStoreConfig(
    postgres.WithEventsTable("my_events"),           // default: "events"
    postgres.WithStreamHeadsTable("my_stream_heads"), // default: "stream_heads"
    postgres.WithLogger(myLogger),                   // optional; nil disables logging
))
```

`postgres.DefaultStoreConfig()` returns a ready-to-use configuration with default table names and no logger.

### NOTIFY Support

Configure the store to issue a `pg_notify` call inside each `Append` transaction. The notification fires only when the transaction commits — no phantom wakes.

```go
s := postgres.NewStore(postgres.NewStoreConfig(
    postgres.WithNotifyChannel("eventsalsa_events"),
))
```

Subscribers can `LISTEN` on the same channel to wake up immediately instead of polling on a fixed interval.

## Migration Generator

`cmd/migrate-gen` generates a single `.sql` file that creates all required tables and indexes.

**Standard Tables (Default):**

```bash
go run eventsalsa.dev/store/cmd/migrate-gen -output migrations
# writes migrations/20060102150405_init_event_sourcing.sql
```

**Native Range Partitioning:**

```bash
go run eventsalsa.dev/store/cmd/migrate-gen \
  -output migrations \
  -partition-strategy native \
  -partition-size 10000000 \
  -initial-partitions 4
```

**`pg_partman` Managed Partitioning:**

```bash
go run eventsalsa.dev/store/cmd/migrate-gen \
  -output migrations \
  -partition-strategy partman \
  -partman-maintenance pg_cron
```

For full DBA guides, maintenance automation, and background worker (`pg_partman_bgw`) setup, see [Range Partitioning Guide](docs/partitioning.md).

**`go:generate`:**

```go
//go:generate go run eventsalsa.dev/store/cmd/migrate-gen -output migrations -filename init.sql
```

The generated migration creates:

- **`events`** — append-only event log with `global_position BIGSERIAL` (or sequence-backed in partitioned mode) primary key
- **`stream_heads`** — one row per stream tracking its current version for atomic stream version reservation and optimistic concurrency during `Append`

## Event Mapping Code Generator

`cmd/eventmap-gen` generates type-safe mapping code between your domain event structs and `store.Event` / `store.PersistedEvent`. This keeps your domain model free of infrastructure dependencies.

```bash
go run eventsalsa.dev/store/cmd/eventmap-gen \
  -input internal/domain/events \
  -output internal/infrastructure/generated
```

See the [`eventmap-codegen`](./examples/eventmap-codegen/) example for a complete demonstration including versioned events and schema evolution patterns.

## Examples

Complete, runnable examples are in [`examples/`](./examples/):

- **[basic](./examples/basic/)** — connecting, appending events, reading streams, and reading the global log

- **[eventmap-codegen](./examples/eventmap-codegen/)** — generating type-safe domain event mappings with `eventmap-gen`, including versioned payloads and projections

## Development

**Unit tests:**

```bash
make test-unit
```

**Integration tests (requires Docker):**

```bash
make test-integration
```

Integration tests automatically launch and manage an isolated PostgreSQL container via [`testcontainers-go`](https://github.com/testcontainers/testcontainers-go).

Or run via `go test` directly:

```bash
go test -p 1 -v -tags=integration ./...
```

**Lint and format:**

```bash
make lint
make fmt
```

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.
