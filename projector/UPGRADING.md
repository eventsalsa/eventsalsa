# Upgrading `eventsalsa/projector`

## Upgrading from `v0.6.0` to `v0.7.0`

`v0.7.0` replaces sequence-gap detection and safe-harbor skipping with PostgreSQL MVCC visibility-horizon consumption (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) ordered and checkpointed by composite `store.Cursor` (`(TransactionID, GlobalPosition)`).

This release requires [`eventsalsa.dev/store` `v0.4.0+`](../store/UPGRADING.md) and a PostgreSQL `events` table with the `transaction_id xid8` column and `(transaction_id, global_position)` index introduced in `store` `v0.3.0`.

### 1. Schema migration (`v0.6.0` -> `v0.7.0`)

`projection_checkpoints` gains `last_transaction_id xid8 NOT NULL DEFAULT '0'`, and `projection_gap_skips` is removed.

To upgrade an existing deployment safely:

```sql
-- Step 1: Add composite transaction cursor column alongside last_position
ALTER TABLE projection_checkpoints
    ADD COLUMN IF NOT EXISTS last_transaction_id xid8 NOT NULL DEFAULT '0';

-- Step 2: Backfill last_transaction_id from events for existing checkpoints.
-- Rows at position 0 or pointing at pruned/pre-v0.3 ('0'::xid8) events retain '0'::xid8.
UPDATE projection_checkpoints pc
SET last_transaction_id = COALESCE(
    (SELECT e.transaction_id FROM events e WHERE e.global_position = pc.last_position),
    '0'::xid8
)
WHERE pc.last_position > 0 AND pc.last_transaction_id = '0'::xid8;
```

Roll out Step 1 and Step 2, deploy the `v0.7.0` projector daemons, and then drop the removed gap-skip audit table:

```sql
-- Step 3: Remove obsolete gap-skip audit table after v0.6.0 daemons are stopped
DROP TABLE IF EXISTS projection_gap_skips;
```

**Why historical backfill is safe:** Even if historical events were originally persisted under `store <= v0.2.0` and backfilled to `transaction_id = '0'::xid8`, setting `last_transaction_id = '0'::xid8` preserves exact resume semantics because `(transaction_id, global_position) > ('0'::xid8, N)` continues sequentially across `('0'::xid8, N+1)` and seamlessly into live transactions (`xid > 0`).

### 2. Breaking API & configuration changes

- **Removed daemon options and `Config` fields**:
  - `WithStaleGapThreshold` / `Config.StaleGapThreshold`
  - `WithStaleGapHarborLag` / `Config.StaleGapHarborLag`
  - `WithProjectionGapSkipsTable` / `Config.ProjectionGapSkipsTable`
- **Migration generator changes (`projector/migrations` and `cmd/migrate-gen`)**:
  - Removed `Config.ProjectionGapSkipsTable` and the `-projection-gap-skips-table` CLI flag.
- **Observer changes**:
  - Removed `Observer.OnGapDetected`, `Observer.OnGapSkipped`, `GapStats`, and `BatchStats.StaleSkipped`.
  - Added `BatchStats.StartTransactionID`, `BatchStats.LastTransactionID`, `BatchStats.HeadTransactionID`, and `PoisonBatchStats.TargetTransactionID`.
- **PostgreSQL checkpoint DAL (`projector/postgres`)**:
  - Removed `DefaultProjectionGapSkipsTable` and gap-skip audit helpers (`GapSkip`, `InsertGapSkip`).
  - `GetCheckpoint`, `GetCheckpointForUpdate`, and `SaveCheckpoint` now accept and return `store.Cursor` (`(TransactionID, GlobalPosition)`) instead of `int64`.
- **Dispatcher (`projector/dispatcher`)**:
  - Replaced `PositionQuerier` (`GetLatestGlobalPosition`) with `HeadQuerier` (`GetHead(ctx, tx) (store.Cursor, error)`).

### 3. Custom read-model replay guards

If your projections guard multi-stream read-model upserts with a scalar `last_global_position < EXCLUDED.last_global_position`, update those tables and upserts to compare the composite cursor `(last_transaction_id, last_global_position) < (EXCLUDED.last_transaction_id, EXCLUDED.last_global_position)` so late-committing transactions with lower `global_position` values are never dropped as stale.

### 4. Operational notes

- **PgBouncer transaction pooling**: MVCC visibility-horizon filtering uses per-transaction SQL snapshots and `xid8` cursors without session state. Pair `LeaderStrategyLease` with `DispatcherStrategyPoll` and a transaction-pool-compatible `pgx` execution mode (such as `QueryExecModeSimpleProtocol`, `Exec`, `DescribeExec`, or `CacheDescribe`; avoid `CacheStatement`).
- **Cluster-wide `xmin` visibility pinning**: Because PostgreSQL's `pg_snapshot_xmin(pg_current_snapshot())` reflects the oldest active transaction holding a transaction ID across the database cluster, any long-running or `idle in transaction` write session with an assigned XID will pause projection visibility progression past that XID until it commits or rolls back. Keep event-store write transactions short and configure `idle_in_transaction_session_timeout` in production.
