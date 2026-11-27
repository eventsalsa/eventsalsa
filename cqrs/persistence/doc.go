// Package persistence provides database-agnostic Decider, Repository, and command handler
// abstractions connecting pure domain decision models to state-based or event-sourced stores.
//
// Core types:
//   - Decider[S, E]: pure Initial(id) and Evolve(state, event) domain functions.
//   - StateRepository[S]: Load/Save persistence port returning ErrNotFound when absent.
//   - EventRepository[E]: stream Load/Append persistence port with optimistic concurrency.
//   - Runner[S, E], NewStateRunner, NewEventRunner, NewCommandHandler: binds pure command
//     decision functions onto eventsalsa.dev/cqrs/command handlers without repository boilerplate.
package persistence
