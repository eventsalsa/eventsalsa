# eventsalsa/cqrs Examples

Runnable examples illustrating how `eventsalsa.dev/cqrs` stays decoupled from persistence and transport choices:

- **[`simple`](./simple/)** — CQRS **without Event Sourcing**. Splits write-side commands (`cqrs/command`, flat `Handler` structs, rejection masking, retries, observability) from lightweight presentation queries (`cqrs/query`) over an in-memory relational store (`db.go`, `commands.go`, `queries.go`, `main.go`). Runs with zero external dependencies:

  ```bash
  go run ./simple
  ```

- **[`event-sourcing`](./event-sourcing/)** — CQRS combined with **Event Sourcing** using [`eventsalsa.dev/store`](../../store/) on PostgreSQL and pure aggregate specs tested via [`eventsalsa.dev/cqrs/spectest`](../spectest/). Organized across focused files (`domain.go`, `tx.go`, `store.go`, `projection.go`, `commands.go`, `queries.go`, `main.go`):

  ```bash
  # Run pure Given/When/Then domain specs without PostgreSQL
  go test -race -count=1 ./event-sourcing/...

  # Run full PostgreSQL example (creates schema tables if missing)
  DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" go run ./event-sourcing
  ```
