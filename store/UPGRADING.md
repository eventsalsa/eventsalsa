# Upgrading Guide

## Upgrading from `v0.3.0` to `v0.4.0`

`v0.4.0` upgrades global-log reading to use PostgreSQL's MVCC transaction visibility horizon (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) and composite `(TransactionID, GlobalPosition)` keyset pagination via `store.Cursor`. No SQL schema changes are required beyond the `v0.3.0` `transaction_id xid8` column and `idx_events_transaction_position` index.

### Breaking API Changes

1. **New `store.Cursor` value type**:
   - Replaces raw `int64` global-position cursors for global log tailing:
     ```go
     type Cursor struct {
         TransactionID  uint64
         GlobalPosition int64
     }
     ```
   - Exposes `Before(other Cursor) bool` and `IsZero() bool`.
2. **`EventReader.ReadEvents` signature updated**:
   - Before (`v0.3.0`): `ReadEvents(ctx context.Context, tx pgx.Tx, fromPosition int64, limit int) ([]PersistedEvent, error)`
   - After (`v0.4.0`): `ReadEvents(ctx context.Context, tx pgx.Tx, after Cursor, limit int) ([]PersistedEvent, error)`
   - Queries only committed events whose `transaction_id` precedes `pg_snapshot_xmin(pg_current_snapshot())`, ordered by `(transaction_id ASC, global_position ASC)` after `after`.
   - Note that `StreamReader.ReadStream` is unchanged and continues reading both committed and intra-transaction uncommitted writes for aggregate reconstruction.
3. **`GlobalPositionReader` replaced by `HeadReader`**:
   - Before (`v0.3.0`): `GlobalPositionReader.GetLatestGlobalPosition(ctx context.Context, tx pgx.Tx) (int64, error)`
   - After (`v0.4.0`): `HeadReader.GetHead(ctx context.Context, tx pgx.Tx) (Cursor, error)`
   - Returns the highest `store.Cursor` currently settled behind `pg_snapshot_xmin(pg_current_snapshot())` (or zero `store.Cursor` when empty).

---

## Upgrading from `v0.2.0` to `v0.3.0`

`v0.3.0` moves the module path to the consolidated vanity domain (`eventsalsa.dev/store`), adds a PostgreSQL transaction identifier (`transaction_id xid8`) to the `events` schema and `store.PersistedEvent`, exports `migrations.SQL`, and fixes schema-prefixed index names in generated migrations.

Because `store` is still in the `0.*` pre-1.0 series, the maintainers updated the initial SQL template in `store/migrations` in place alongside this upgrade runbook for existing deployments.

---

### Step 1: Update Go Import Paths & `PersistedEvent` Expectations

1. Update your `go.mod` and `.go` imports from `github.com/eventsalsa/store` to `eventsalsa.dev/store`:
   ```bash
   go get eventsalsa.dev/store@v0.3.0
   ```
2. `store.PersistedEvent` now exposes `TransactionID uint64` populated on both `Append` (`AppendResult.Events`) and read methods (`ReadEvents`, `ReadStream`).

---

### Step 2: Upgrade Existing PostgreSQL `events` Table

Choose **Option A** for development/small tables (brief table rewrite lock), or **Option B** for zero-downtime production upgrades (PostgreSQL 12+). Replace `events` with your configured schema-qualified table name (for example `ordering.events`) if customized.

#### Option A: Maintenance Window (Single Transaction)

```sql
BEGIN;

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id();

CREATE INDEX IF NOT EXISTS idx_events_transaction_position
    ON events (transaction_id, global_position);

COMMIT;
```

> **Partitioned tables:** Running the statements above on a `PARTITION BY RANGE (global_position)` parent table automatically propagates the column and index to all existing partitions.

---

#### Option B: Online Zero-Downtime Upgrade (PostgreSQL 12+)

##### 1. Unpartitioned `events` Table

```sql
-- 1. Metadata-only column add + volatile default for new inserts (brief lock only)
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS transaction_id xid8;

ALTER TABLE events
    ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id();

-- 2. Backfill historical pre-upgrade rows in batches using '0'::xid8
-- ('0'::xid8 strictly precedes every normal PostgreSQL xid8 >= 3, preserving
-- ordering relative to concurrent post-upgrade writes). Repeat until 0 rows update:
WITH batch AS (
    SELECT global_position
    FROM events
    WHERE transaction_id IS NULL
    ORDER BY global_position
    LIMIT 50000
)
UPDATE events e
SET transaction_id = '0'::xid8
FROM batch
WHERE e.global_position = batch.global_position;

-- 3. Add and validate a NOT NULL CHECK constraint without blocking concurrent writes
ALTER TABLE events
    ADD CONSTRAINT events_transaction_id_not_null
    CHECK (transaction_id IS NOT NULL) NOT VALID;

ALTER TABLE events
    VALIDATE CONSTRAINT events_transaction_id_not_null;

-- PostgreSQL skips a full table scan here because the validated CHECK proves no NULLs exist
ALTER TABLE events
    ALTER COLUMN transaction_id SET NOT NULL;

ALTER TABLE events
    DROP CONSTRAINT events_transaction_id_not_null;

-- 4. Build the composite index concurrently (must run outside an explicit transaction block)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_transaction_position
    ON events (transaction_id, global_position);
```

##### 2. Declarative / `pg_partman` Partitioned `events` Table

1. Run steps **1–3** above against the parent `events` table (PostgreSQL propagates the nullable column, default, and `NOT VALID` constraint, then `VALIDATE CONSTRAINT` scans partitions without holding an `ACCESS EXCLUSIVE` lock).
2. Create the invalid parent index and attach per-partition indexes built `CONCURRENTLY`:

```sql
-- 1. Create parent index metadata without recursively locking child partitions
CREATE INDEX IF NOT EXISTS idx_events_transaction_position
    ON ONLY events (transaction_id, global_position);

-- 2. For EACH existing child partition (repeat per partition outside a tx block):
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_p0000000001_p0010000000_transaction_position
    ON events_p0000000001_p0010000000 (transaction_id, global_position);

ALTER INDEX idx_events_transaction_position
    ATTACH PARTITION idx_events_p0000000001_p0010000000_transaction_position;
```

Once all child partitions are attached, PostgreSQL automatically marks `idx_events_transaction_position` as valid, and future partitions created via declarative partitioning or `pg_partman` inherit both `transaction_id` and `idx_events_transaction_position` automatically.
