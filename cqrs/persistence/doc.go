// Package persistence provides database-agnostic Decider, Repository, Adapter, and
// command handler abstractions connecting pure domain decision models to state-based or
// event-sourced stores with optimistic concurrency control.
//
// Core types:
//   - Decider[S, E]: pure Initial(id) and Evolve(state, event) domain functions.
//   - StateRepository[S]: Load/Save persistence port with expectedVersion OCC checks.
//   - EventRepository[E]: stream Load/Append persistence port with expectedVersion OCC checks.
//   - Adapter[S, E]: unified persistence binding created via FromStateRepository,
//     FromEventRepository, or NewAdapter.
//   - NewCommandHandler: adapts a pure decision function over an Adapter[S, E] onto
//     eventsalsa.dev/cqrs/command.Handler[C].
package persistence
