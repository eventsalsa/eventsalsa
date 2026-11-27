# eventsalsa/cqrs Examples

Runnable examples illustrating how `eventsalsa.dev/cqrs` adapts to different architectural choices while staying decoupled from persistence and transport concerns.

| Example | Pattern | Contracts | Description |
| --- | --- | --- | --- |
| [`without-event-sourcing/iteration-1`](./without-event-sourcing/iteration-1/) | Without Event Sourcing | Exported Go structs (`commands/`, `queries/`) | Direct `internal/application` write handlers, `internal/domain` entity invariants, and thin `internal/infra` read handlers. Zero external dependencies. |
| [`without-event-sourcing/iteration-2`](./without-event-sourcing/iteration-2/) | Without Event Sourcing | Compiled Protobuf (`commands/`, `queries/`) | `command.Func` ACL adapters in `internal/infra/acl`, internal `internal/application` commands & handlers, `internal/domain` entity invariants, and Protobuf read views. |
| [`without-event-sourcing/iteration-3`](./without-event-sourcing/iteration-3/) | Without Event Sourcing | Compiled Protobuf (`commands/`, `queries/`) | `command.Func` ACL adapters, single generic `NewDeciderHandler` in `internal/application`, pure `Initial`/`Decide`/`Evolve` Decider in `internal/domain` tested with `cqrs/spectest`, and Protobuf read views. |
| [`event-sourcing`](./event-sourcing/) | With `eventsalsa/store` Event Sourcing | Go structs | Append-only event persistence on PostgreSQL paired with inline read projections and `cqrs/spectest` specs. |

## Running the Examples

```bash
# CQRS without Event Sourcing (Iterations 1-3)
go run ./without-event-sourcing/iteration-1
go run ./without-event-sourcing/iteration-2
go run ./without-event-sourcing/iteration-3
go test -race -count=1 ./without-event-sourcing/...

# Pure Given/When/Then domain specs without PostgreSQL
go test -race -count=1 ./event-sourcing/...

# Full PostgreSQL Event Sourcing example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" go run ./event-sourcing
```
