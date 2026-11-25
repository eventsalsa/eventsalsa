# Upgrading Guide

## Upgrading from `v0.4.0` to `v0.5.0` *(unreleased)*

`v0.5.0` simplifies infrastructure causal metadata to **`TraceID`** and **`CausationID`** (removing `CorrelationID`), adds a partial `trace_id` index (`idx_events_trace`) across all migration strategies, and introduces helper APIs (`ExpectedVersion.Matches`, plus `MarshalEvent` and `FromESEventAs[T]` in `eventmap-gen`) for higher-level command handling.

### Rationale: Two Causal Fields Instead of Three

- **`TraceID`** groups every synchronous and asynchronous hop of an end-to-end execution flow.
- **`CausationID`** links each event or outbound message directly to its immediate cause (`CommandID`, upstream `EventID`, or inbound `MessageID`).
- **Saga / business correlation belongs in explicit domain event payloads** (`OrderID`, `TransferID`, `PaymentID`), never in infrastructure metadata. External webhooks, timeouts, or manual retries enter workflows under a new `TraceID`, so routing sagas on domain identifiers keeps business rules decoupled from transport envelopes and eliminates write amplification from the previously unused `idx_events_correlation` index.
- Custom caller/transport identifiers (such as `X-Request-Id`) belong in JSONB `Metadata`.

### Breaking API & Schema Changes

1. **Removed `CorrelationID` (`events.correlation_id` and `idx_events_correlation`):**
   - `store.Event.CorrelationID` and `store.PersistedEvent.CorrelationID` have been removed.
   - Generated migrations in `store/migrations` (`none`, `native`, `partman`) drop `correlation_id TEXT` and replace `idx_<table>_correlation` with the partial index `idx_<table>_trace ON <table> (trace_id) WHERE trace_id IS NOT NULL`.
   - `cmd/eventmap-gen` no longer emits `WithCorrelationID`.
2. **Distinct `ErrDuplicateEventID` Sentinel:**
   - `postgres.Store.Append` now returns `store.ErrDuplicateEventID` when an `event_id` unique constraint violation occurs (previously misclassified as `store.ErrOptimisticConcurrency`).
3. **Additive Helpers & Behavioral Improvements (Non-Breaking):**
   - `ExpectedVersion.Matches(currentVersion int64) bool` validates expected versions (`Any`, `NoStream`, `Exact(n)`) against a loaded stream version.
   - `store.ErrUnknownEvent` is now wrapped by `cmd/eventmap-gen` helpers (`EventTypeOf`, `MarshalEvent`, `ToESEvents`, `FromESEvent`) for unregistered event types, unknown versions, and nil/typed-nil events.
   - `cmd/eventmap-gen` now emits `MarshalEvent(e any) (eventType string, version int, payload []byte, err error)` and `FromESEventAs[T any](pe store.PersistedEvent) (T, error)`, and resolves colliding domain package names with deterministic import aliases.
   - `postgres.Store.Append` uses `INSERT ... ON CONFLICT (stream_type, stream_id) DO NOTHING` on `stream_heads` for `store.NoStream()` / `store.Exact(0)` so stream-creation conflicts return `store.ErrOptimisticConcurrency` without aborting the caller's PostgreSQL transaction (`25P02`).

### Zero-Downtime Database Upgrade (`v0.4.0` -> `v0.5.0`)

Because `correlation_id` was nullable and `store` no longer inserts or selects it, `v0.5.0` binaries remain compatible with existing `v0.4.0` tables. Follow this order for zero-downtime rollouts (replace `events` with your schema-qualified table name if customized):

1. **Deploy `v0.5.0` application instances first** so no running code reads or writes `correlation_id`.
2. **Create `idx_events_trace` online:**
   - **Unpartitioned `events` table:**
     ```sql
     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_trace
         ON events (trace_id) WHERE trace_id IS NOT NULL;
     ```
   - **Declarative / `pg_partman` partitioned `events` table** (create parent metadata index, build `CONCURRENTLY` per partition, then attach):
     ```sql
     CREATE INDEX IF NOT EXISTS idx_events_trace
         ON ONLY events (trace_id) WHERE trace_id IS NOT NULL;

     -- Repeat per existing child partition outside a transaction block:
     CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_p0000000001_p0010000000_trace
         ON events_p0000000001_p0010000000 (trace_id) WHERE trace_id IS NOT NULL;

     ALTER INDEX idx_events_trace
         ATTACH PARTITION idx_events_p0000000001_p0010000000_trace;
     ```
3. **Optionally drop the legacy correlation index and column** once rollbacks to `v0.4.0` are no longer needed:
   - **Unpartitioned `events` table:**
     ```sql
     DROP INDEX CONCURRENTLY IF EXISTS idx_events_correlation;
     ALTER TABLE events DROP COLUMN IF EXISTS correlation_id;
     ```
   - **Declarative / `pg_partman` partitioned `events` table** (`DROP INDEX CONCURRENTLY` is not supported on partitioned parent indexes; run sequentially or drop per-partition indexes first):
     ```sql
     DROP INDEX IF EXISTS idx_events_correlation;
     ALTER TABLE events DROP COLUMN IF EXISTS correlation_id;
     ```

---

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
