# CQRS Without Event Sourcing (`cqrs/examples/without-event-sourcing`)

An iterative evolution showing three idiomatic Go repository layouts for adopting `eventsalsa.dev/cqrs` over standard relational/state persistence without Event Sourcing:

| Iteration | Storage Model | Architecture Highlights |
| --- | --- | --- |
| [`iteration-1`](./iteration-1/) | Single in-memory DB | Direct exported Go `commands`/`queries` structs; `internal/domain` entity invariants; `internal/application` write handlers; `internal/infra` persistence and read handlers. |
| [`iteration-2`](./iteration-2/) | Single in-memory DB | Top-level Protobuf `commands`/`queries` contracts; `command.Func` ACL adapters in `internal/infra/acl` mapping to `internal/application` use-case commands; `internal/domain` entity invariants. |
| [`iteration-3`](./iteration-3/) | Single in-memory DB | Top-level Protobuf `commands`/`queries` contracts; `command.Func` ACL adapters; `eventsalsa.dev/cqrs/persistence` generic `Decider`/`FromStateRepository`/`NewCommandHandler` with state OCC; pure `Initial`/`Decide`/`Evolve` Decider in `internal/domain` tested with `cqrs/spectest`. |

Notice how all three iterations:
- Expose top-level `commands/` and `queries/` packages cleanly separated from `internal/` packages (in Go, anything outside `internal/` is already public—no `public/` wrapper package needed).
- Isolate domain invariants and internal write plumbing inside `internal/` so external callers cannot bypass boundaries.
- Keep write handlers decoupled from `queries/` so write paths never import read contracts.
- Place query handlers directly in `internal/infra/` because queries are thin DB-to-DTO adapters.
