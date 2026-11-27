# CQRS With Event Sourcing (`cqrs/examples/with-event-sourcing`)

An iterative series showing how `eventsalsa.dev/cqrs` combines with Event Sourcing (`eventsalsa.dev/store` on PostgreSQL) and pure `Given / When / Then` domain specifications (`eventsalsa.dev/cqrs/spectest`).

| Iteration | Storage Model | Architecture Highlights |
| --- | --- | --- |
| [`iteration-1`](./iteration-1/) | PostgreSQL event log (`eventsalsa/store`) + `order_overview_v1` | Exported Go `commands`/`queries` structs; pure `internal/domain` event-sourced state & rules tested with `cqrs/spectest`; `internal/application` handlers over `OrderEventStore`; `internal/infra` `pgx` tx middleware, `eventmap-gen` mappings, inline projection, and SQL read handler. |

Notice how `iteration-1` echoes [`../without-event-sourcing/iteration-1/`](../without-event-sourcing/iteration-1/):
- Top-level `commands/` and `queries/` packages are exported directly as plain Go structs.
- `internal/domain/` has zero `store`, `pgx`, or `cqrs/command` imports.
- `internal/application/` orchestrates writes behind an `OrderEventStore` port and never imports `queries/` or `internal/infra`.
- `internal/infra/` isolates PostgreSQL transactions, event encoding, inline read-model updates, and SQL read queries.
