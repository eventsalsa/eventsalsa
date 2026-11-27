# eventsalsa/cqrs Examples

Runnable examples illustrating how `eventsalsa.dev/cqrs` adapts to different architectural choices while staying decoupled from persistence and transport concerns.

| Example | Pattern | Contracts | Description |
| --- | --- | --- | --- |
| [`without-event-sourcing/iteration-1`](./without-event-sourcing/iteration-1/) | Without Event Sourcing | Exported Go structs (`commands/`, `queries/`) | Direct `internal/application` write handlers, `internal/domain` entity invariants, and thin `internal/infra` read handlers. Zero external dependencies. |
| [`without-event-sourcing/iteration-2`](./without-event-sourcing/iteration-2/) | Without Event Sourcing | Compiled Protobuf (`commands/`, `queries/`) | `command.Func` ACL adapters in `internal/infra/acl`, internal `internal/application` commands & handlers, `internal/domain` entity invariants, and Protobuf read views. |
| [`without-event-sourcing/iteration-3`](./without-event-sourcing/iteration-3/) | Without Event Sourcing | Compiled Protobuf (`commands/`, `queries/`) | `command.Func` ACL adapters, generic `Decider`/`StateRepository` handler in `internal/infra/persistence`, pure `Initial`/`Decide`/`Evolve` Decider in `internal/domain` tested with `cqrs/spectest`, and Protobuf read views. |
| [`with-event-sourcing/iteration-1`](./with-event-sourcing/iteration-1/) | With `eventsalsa/store` Event Sourcing | Exported Go structs (`commands/`, `queries/`) | `Order` aggregate & `OrderRepository` (`Load`/`Save`) in `internal/domain`, write handlers in `internal/application`, and `internal/infra` PostgreSQL tx + `eventmap-gen` mappings + `eventsalsa/store` + inline projection. |

## Running the Examples

```bash
# CQRS without Event Sourcing (Iterations 1-3)
go run ./without-event-sourcing/iteration-1
go run ./without-event-sourcing/iteration-2
go run ./without-event-sourcing/iteration-3
go test -race -count=1 ./without-event-sourcing/...

# Pure domain & event-mapping unit tests without PostgreSQL
go test -race -count=1 ./with-event-sourcing/...

# Full PostgreSQL Event Sourcing example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" \
  go run ./with-event-sourcing/iteration-1
```
