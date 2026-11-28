// Package postgres provides PostgreSQL-native transaction scoping, EventRepository and
// TxEventRepository wiring over eventsalsa.dev/store, inline projection hooks, event mapping
// helpers, and causal event bridging for eventsalsa.dev/cqrs/persistence.
//
// Explicit transaction ownership (no silent transaction inheritance):
//   - Every transaction starter (InReadTx, InTx, NewTxMiddleware, NewQueryTxMiddleware,
//     AdoptTx) is strictly non-reentrant and fails immediately with ErrTxAlreadyActive if a
//     transaction is already active in context.Context.
//   - Narrow by default: EventRepository (NewEventRepository) owns its own short pgx.ReadOnly
//     transaction on Load and short read-write transaction on Append, releasing read locks
//     before domain decision logic runs. Calling narrow Load/Append inside an active context
//     transaction fails loudly with ErrTxAlreadyActive.
//   - Wide by composition: mount NewTxMiddleware / MustTxMiddleware (or AdoptTx) when multi-
//     stream or SQL+stream atomicity is needed, and pair it either with NewTxEventRepository
//     (which uses RequireTx and returns ErrMissingTx if absent) or explicit LoadTx / AppendTx
//     calls with caller-managed pgx.Tx.
//   - Snapshotted event sourcing: NewSnapshotAdapter and NewTxSnapshotAdapter pair
//     eventsalsa.dev/snapshot (periodic Save with savepoint isolation or atomic rollback,
//     upcasters, and payload transformers) with eventsalsa.dev/cqrs/persistence.Adapter.
package postgres
