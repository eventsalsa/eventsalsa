# eventsalsa/cqrs Examples

Runnable examples illustrating how `eventsalsa.dev/cqrs` stays decoupled from storage choices:

- **[`simple`](./simple/)** — Single-database CQRS **without Event Sourcing**. Separates authoritative write-side command logic (`cqrs/command`) from lightweight presentation DTO queries (`cqrs/query`), including domain rejection classification, retries, and causal observability. Runs with zero external dependencies:

  ```bash
  go run ./simple
  ```

- **[`event-sourcing`](./event-sourcing/)** — CQRS combined with **Event Sourcing** using [`eventsalsa.dev/store`](../../store/) on PostgreSQL and pure aggregate specs tested via [`eventsalsa.dev/cqrs/spectest`](../spectest/). Connects `command.Bus` (observability -> optimistic concurrency retry -> transaction boundary -> `decide`/`evolve` -> `store.Append` + inline projection) with `query.Execute` reading the `order_overview_v1` projection table:

  ```bash
  # Run pure Given/When/Then domain specs without PostgreSQL
  go test -race -count=1 ./event-sourcing/...

  # Run full PostgreSQL example (starts schema if missing)
  DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" go run ./event-sourcing
  ```
