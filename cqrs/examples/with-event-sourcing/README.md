# CQRS With Event Sourcing (`cqrs/examples/with-event-sourcing`)

An iterative series showing how `eventsalsa.dev/cqrs` combines with Event Sourcing (`eventsalsa.dev/store` on PostgreSQL) and evolves alongside `eventsalsa`.

| Iteration | Storage Model | Architecture Highlights |
| --- | --- | --- |
| [`iteration-1`](./iteration-1/) | PostgreSQL event log (`eventsalsa/store`) + `order_overview_v1` | Exported Go `commands`/`queries` structs; OOP event-sourced `Order` aggregate & `OrderRepository` (`Load`/`Save`) port in `internal/domain`; `internal/application` handlers; `internal/infra` `pgx` tx middleware, `eventmap-gen` mappings, `OrderOverviewProjection`, and SQL read handler. |
| [`iteration-2`](./iteration-2/) | PostgreSQL event log (`eventsalsa/store`) + `order_overview_v1` | Top-level Protobuf `commands`/`queries` contracts; `command.Func` ACL adapters (`internal/infra/acl`); internal `internal/application` use-case commands/handlers; OOP `Order` aggregate & `OrderRepository`; `internal/infra/{persistence,queries}` with `eventmap-gen` and Protobuf read views. |
| [`iteration-3`](./iteration-3/) | PostgreSQL event log (`eventsalsa/store`) + `order_overview_v1` | Top-level Protobuf `commands`/`queries` contracts; `command.Func` ACL adapters; `eventsalsa.dev/cqrs/persistence` (`FromEventRepository`, `NewCommandHandler`) wired via `eventsalsa.dev/cqrs/persistence/postgres` (`NewEventRepository`, `NewEventMapCodec`, narrow-by-default txs); pure `Initial`/`Decide`/`Evolve` Decider in `internal/domain` tested with `cqrs/spectest` (no `internal/application` boilerplate). |
| [`iteration-4`](./iteration-4/) | PostgreSQL event log + `snapshots` (`eventsalsa/snapshot`) + `order_overview_v1` | Extends `iteration-3` with repeating `AddOrderItem` commands, top-level [`config/`](./iteration-4/config/) env/validation, [`bootstrap/`](./iteration-4/bootstrap/) composition root, `NewOrderSnapshotAdapter` options, and ACL boundary translators. |

Notice how each iteration echoes [`../without-event-sourcing/`](../without-event-sourcing/):
- `iteration-1` exposes plain Go `commands/` and `queries/` contracts directly with minimal layers.
- `iteration-2` introduces compiled Protobuf boundary contracts, `internal/infra/acl` `command.Func` translators, and separated `internal/application` use-case commands.
- `iteration-3` adopts `eventsalsa.dev/cqrs/persistence` (`FromEventRepository`, `NewCommandHandler`) + `eventsalsa.dev/cqrs/persistence/postgres` (`NewEventRepository`, `NewEventMapCodec`) and pure `Initial`/`Decide`/`Evolve` Decider specs tested via `cqrs/spectest`.
- `iteration-4` layers `eventsalsa.dev/snapshot` (`NewSnapshotAdapter` + `snapshot.EveryNEvents(3)` + `Gzip`) onto the same `Decider`, ACLs, and `NewCommandHandler` pattern.
- `internal/domain/` always keeps zero `store`, `snapshot`, `pgx`, Protobuf, or `cqrs/command` imports.
