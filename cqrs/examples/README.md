# eventsalsa/cqrs Examples

Runnable examples illustrating how `eventsalsa.dev/cqrs` adapts to different architectural styles while staying decoupled from persistence and transport choices.

| Example | Pattern | Contracts | Description |
| --- | --- | --- | --- |
| [`without-event-sourcing/style-1`](./without-event-sourcing/style-1/) | Without Event Sourcing | Exported Go structs (`commands/`, `queries/`) | Direct `internal/application` write handlers, `internal/domain` invariants, and thin `internal/infra` read handlers. Zero external dependencies. |
| [`without-event-sourcing/style-2`](./without-event-sourcing/style-2/) | Without Event Sourcing | Compiled Protobuf (`commands/`, `queries/`) | Private domain commands in `internal/domain`, `command.Func` ACL adapters in `internal/infra/acl`, `internal/application` write handlers, and Protobuf read views. |
| [`event-sourcing`](./event-sourcing/) | With `eventsalsa/store` Event Sourcing | Go structs | Append-only event persistence on PostgreSQL paired with inline read projections and `cqrs/spectest` specs. |

## Running the Examples

```bash
# CQRS without Event Sourcing (Style 1 & Style 2)
go run ./without-event-sourcing/style-1
go run ./without-event-sourcing/style-2
go test -race -count=1 ./without-event-sourcing/...

# Pure Given/When/Then domain specs without PostgreSQL
go test -race -count=1 ./event-sourcing/...

# Full PostgreSQL Event Sourcing example (creates schema tables if missing)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" go run ./event-sourcing
```
