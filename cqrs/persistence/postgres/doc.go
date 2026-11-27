// Package postgres provides PostgreSQL-native transaction scoping, EventRepository
// wiring over eventsalsa.dev/store, inline projection hooks, event mapping helpers, and
// causal event bridging for eventsalsa.dev/cqrs/persistence.
//
// Transaction model (narrow by default, wide by composition):
//   - EventRepository.Load and InReadTx run inside a short pgx.ReadOnly transaction that
//     closes before domain decision logic executes, avoiding held locks or open write
//     transactions during decide(state, cmd).
//   - EventRepository.Append and InTx open a short read-write transaction strictly around
//     database mutations (store.EventStore.Append plus inline projections, or StateRepository
//     Save) and commit immediately.
//   - When multi-stream atomicity or cross-step read-your-writes isolation is needed, mount
//     NewTxMiddleware / MustTxMiddleware on command.Bus (or wrap ctx via AdoptTx). Both
//     InReadTx and InTx automatically detect and reuse an active context transaction.
//   - Transaction starters (NewTxMiddleware, NewQueryTxMiddleware, AdoptTx) are strictly
//     non-reentrant and fail with ErrTxAlreadyActive if nested.
package postgres
