# CQRS With Event Sourcing (`cqrs/examples/with-event-sourcing`)

An iterative series showing how `eventsalsa.dev/cqrs` combines with Event Sourcing (`eventsalsa.dev/store` on PostgreSQL) and evolves alongside `eventsalsa`.

| Iteration | Storage Model | Architecture Highlights |
| --- | --- | --- |
| [`iteration-1`](./iteration-1/) | PostgreSQL event log (`eventsalsa/store`) + `order_overview_v1` | Exported Go `commands`/`queries` structs; OOP event-sourced `Order` aggregate & `OrderRepository` (`Load`/`Save`) port in `internal/domain`; `internal/application` handlers; `internal/infra` `pgx` tx middleware, `eventmap-gen` mappings, `OrderOverviewProjection`, and SQL read handler. |

Notice how `iteration-1` echoes [`../without-event-sourcing/iteration-1/`](../without-event-sourcing/iteration-1/):
- Top-level `commands/` and `queries/` packages are exported directly as plain Go structs.
- `internal/domain/` defines `Order` and `OrderRepository` (`Load`/`Save`) with zero `store`, `pgx`, or `cqrs/command` imports.
- `internal/application/` orchestrates domain calls through `domain.OrderRepository` and never handles event slices, stream versions, `queries/`, or `internal/infra`.
- `internal/infra/` isolates PostgreSQL transactions, `ReconstituteOrder` + `Evolve` event loading, `eventmap-gen` encoding, inline read-model updates, and SQL read queries.
