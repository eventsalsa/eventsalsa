# CQRS Without Event Sourcing (`cqrs/examples/without-event-sourcing`)

Two idiomatic Go repository layouts for adopting `eventsalsa.dev/cqrs` over standard relational/state persistence without Event Sourcing:

| Style | Directory | Commands live in | Transport / Boundary | Query handlers live in |
| :--- | :--- | :--- | :--- | :--- |
| **Style 1** | [`style-1/`](./style-1/) | Top-level `commands/` package | Callers construct and dispatch `commands.*` directly | `internal/infra/` |
| **Style 2** | [`style-2/`](./style-2/) | `internal/domain/` | Top-level Protobuf contracts in `commands/` & `queries/`; `internal/infra/acl` maps protobuf -> `domain` commands via `command.Func` | `internal/infra/queries/` |

Notice how both styles:
- Expose top-level `commands/` and `queries/` packages cleanly separated from `internal/` packages (in Go, anything outside `internal/` is already public—no `public/` wrapper package needed).
- Isolate domain invariants and application handlers inside `internal/` so external callers cannot bypass boundaries.
- Keep `internal/application` completely decoupled from `queries/` so write handlers never import read contracts.
- Place query handlers directly in `internal/infra/` because queries are thin DB-to-DTO adapters.
