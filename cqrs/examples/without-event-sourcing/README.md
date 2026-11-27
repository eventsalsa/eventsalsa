# CQRS Without Event Sourcing (`cqrs/examples/without-event-sourcing`)

Two idiomatic Go repository layouts for adopting `eventsalsa.dev/cqrs` over standard relational/state persistence without Event Sourcing:

| Example | Storage Model | Architecture Highlights |
| --- | --- | --- |
| [`style-1`](./style-1/) | Single in-memory DB | Direct exported Go `commands`/`queries` structs; `internal/domain` invariants; `internal/application` write handlers; `internal/infra` persistence and read handlers. |
| [`style-2`](./style-2/) | Single in-memory DB | Top-level Protobuf `commands`/`queries` contracts; `command.Func` ACL adapters in `internal/infra/acl`; thin `internal/application` orchestration delegating to pure `internal/domain` command handlers. |

Notice how both styles:
- Expose top-level `commands/` and `queries/` packages cleanly separated from `internal/` packages (in Go, anything outside `internal/` is already public—no `public/` wrapper package needed).
- Isolate domain invariants and application handlers inside `internal/` so external callers cannot bypass boundaries.
- Keep `internal/application` completely decoupled from `queries/` so write handlers never import read contracts.
- Place query handlers directly in `internal/infra/` because queries are thin DB-to-DTO adapters.
