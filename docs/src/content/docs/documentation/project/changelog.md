---
title: Changelog
description: Released versions of the current eventsalsa components.
---

This page summarizes released versions of the current `eventsalsa` components. It is intentionally component-oriented so the notes stay aligned with the published libraries rather than the docs site itself.

> **Monorepo & Vanity Import Cutover:** Starting with `eventsalsa/store` v0.3.0, `eventsalsa/projector` v0.6.0, `eventsalsa/encryption` v0.1.0 (`kek/awskms` v0.0.1, `kek/gcpkms` v0.0.1), and `eventsalsa/snapshot` v0.0.2, all components are developed in `github.com/eventsalsa/eventsalsa` and published under `eventsalsa.dev/*` vanity Go module paths. Historical pre-cutover releases remain archived on their original `github.com/eventsalsa/*` repositories.

## eventsalsa/store

### v0.4.0

- **Breaking Changes**:
  - Replaced `GlobalPositionReader` (`GetLatestGlobalPosition`) with `HeadReader` (`GetHead(ctx, tx) (Cursor, error)`).
  - Updated `EventReader.ReadEvents` to accept a composite `store.Cursor` (`TransactionID`, `GlobalPosition`), filtering strictly by PostgreSQL's MVCC visibility horizon (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) and ordering ascending by `(transaction_id, global_position)`.
- **Features**:
  - Added `store.Cursor` (`Before`, `IsZero`) for composite `(TransactionID, GlobalPosition)` log consumption and checkpointing.

### v0.3.0

- **Breaking Changes**:
  - Migrated Go module import path from `github.com/eventsalsa/store` to `eventsalsa.dev/store`.
- **Features**:
  - Added `transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id()` and `idx_<table>_transaction_position` across all PostgreSQL migration strategies (`none`, `native`, `partman`), and exposed `PersistedEvent.TransactionID` (`uint64`) on appends and reads.
  - Exported `migrations.SQL(config *migrations.Config) (string, error)` for programmatic DDL generation and added `UPGRADING.md` with zero-downtime PostgreSQL upgrade instructions.
- **Fixes**:
  - Fixed schema-prefixed index generation (`ordering.events`) in `store/migrations` and added `CREATE SCHEMA IF NOT EXISTS` statements when schemas are specified.

### v0.2.0

- **Breaking Changes**:
  - Removed `Consumer` and `ScopedConsumer` interfaces from the store package (`github.com/eventsalsa/store/consumer`).

### v0.1.0

- **Breaking Changes**:
  - Replaced `Aggregate` terminology with `Stream` across all core types (`Event.StreamType`, `Event.StreamID`, `PersistedEvent.StreamVersion`, `Stream`), stream reader interfaces (`StreamReader.ReadStream`), configuration (`postgres.WithStreamHeadsTable`), migration tables (`stream_heads`), and CLI tools (`eventmap-gen`, `migrate-gen`).
- **Features**:
  - Added declarative PostgreSQL range partitioning on `global_position` for massive event logs, supporting native pre-allocated partitions and dynamic management with `pg_partman` (via `pg_cron`, background worker `bgw`, or external scheduler).
- **Improvements**:
  - Introduced authoritative `stream_heads` atomic optimistic concurrency control engine with transaction row-level locking.
  - Migrated integration test suite to `testcontainers-go`.
  - Upgraded to `github.com/jackc/pgx/v5` v5.10.0.

### v0.0.4

- **Bug Fixes**:
  - Fixed non-deterministic switch-case generation in `eventmap-gen` and formatted generated code with standard `go/format`.
  - Configured automated releases with Release Please.

### v0.0.3

- Fixed JSONB parameter binding compatibility when using pgx's simple protocol mode, ensuring metadata maps correctly under PgBouncer transaction pooling.

### v0.0.2

- Migrated PostgreSQL event store implementation and interface parameters to native `pgx/v5`, removing the `database/sql` dependency.

### v0.0.1

- Initial release.

## eventsalsa/projector

### v0.7.0

- **Breaking Changes**:
  - Replaced sequence-gap / safe-harbor progression and the `projection_gap_skips` table with PostgreSQL MVCC visibility-horizon consumption (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) and composite `(last_transaction_id, last_position)` checkpoints (`store.Cursor`). Requires `eventsalsa.dev/store` `v0.4.0+`.
  - Removed `WithStaleGapThreshold`, `WithStaleGapHarborLag`, `WithProjectionGapSkipsTable`, `ProjectionGapSkipsTable` migration settings, `-projection-gap-skips-table`, `Observer.OnGapDetected`, `Observer.OnGapSkipped`, `GapStats`, `BatchStats.StaleSkipped`, `DefaultProjectionGapSkipsTable`, and `dispatcher.PositionQuerier` (replaced by `dispatcher.HeadQuerier`).
  - Updated `projector/postgres` checkpoint helpers (`GetCheckpoint`, `GetCheckpointForUpdate`, `SaveCheckpoint`) to use `store.Cursor`.
- **Features**:
  - Added `StartTransactionID`, `LastTransactionID`, and `HeadTransactionID` to `BatchStats`, and `TargetTransactionID` to `PoisonBatchStats`.
- **Bug Fixes**:
  - Allocated a fresh bounded context for leader connection/lease release after forced shutdown drain timeouts so restarted daemons acquire leadership without waiting on expired pool sessions.

### v0.6.0

- **Breaking Changes**:
  - Migrated Go module import paths to `eventsalsa.dev/projector` and `eventsalsa.dev/store` (`v0.3.0`).
- **Improvements**:
  - Upgraded `github.com/jackc/pgx/v5` and toolchain compatibility to Go 1.27.4.

### v0.5.0

- **Breaking Changes**:
  - `projector.New(db, eventStore, registry, opts...)` now accepts a `*projector.Registry` (built with `projector.NewRegistry()` or `projector.FromProjections(...)`) instead of a `[]projector.Projection` slice.
  - Added `OnProjectionDegraded(ctx, DegradedStats)` and `OnPoisonBatchSkipped(ctx, PoisonBatchStats)` methods to the `Observer` interface (`NoopObserver` provides default no-op implementations).
- **Features**:
  - Added support for four projection handler shapes: transactional per-event (`Projection`), transactional batch (`BatchProjection`), detached per-event (`DetachedProjection`), and detached batch (`DetachedBatchProjection`), inferred automatically by `Registry.Add` and `FromProjections`.
  - Added registration-time stream and event type filters (`OnStreamTypes`, `OnEventTypes`) applicable to all four projection shapes.
  - Added detached phase timeout budgets (`WithDetachedApplyTimeout`, `WithCheckpointTimeout`) so external network I/O never holds PostgreSQL connections or starves post-apply checkpoint writes.
  - Added explicit error classification (`failure.Retryable`, `failure.Permanent`), per-projection failure policies (`FailurePolicyFailFast`, `FailurePolicyRetry`, `WithFailurePolicy`), and dead-letter poison batch handlers (`WithPoisonHandler`).
- **Bug Fixes**:
  - Fixed a race condition on the dedicated leader connection during forced daemon shutdown.
  - Isolated the post-apply detached checkpoint transaction into its own timeout budget (`WithCheckpointTimeout`).

### v0.4.0

- **Features**:
  - Refined lifecycle coordination for seamless `golang.org/x/sync/errgroup` integration: `(*Daemon).Start(ctx)` returns `nil` on graceful context cancellation and non-nil on fatal faults.
  - Added configurable `WithShutdownTimeout(d)` for in-flight batch draining during graceful daemon shutdown before forced cancellation.
  - Introduced `(*Daemon).IsRunning()` and `(*Daemon).IsLeader()` state inspection methods for liveness, readiness, and monitoring probes.

### v0.3.0

- **Features**:
  - Introduced pluggable runtime `Observer` interface (`OnBatchProcessed`, `OnHeartbeat`, `OnGapDetected`, `OnGapSkipped`, `OnRebalance`) for real-time telemetry, gap tracking, and Prometheus metrics emission without out-of-band database polling.
  - Added telemetry data structures (`BatchStats`, `DaemonStats`, `GapStats`) and utility wrappers (`NoopObserver`, `MultiObserver`).

### v0.2.0

- **Features**:
  - Introduced canonical `Projection` interface (`Name() string`, `Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error`).
  - Added dynamic projection stream and event filtering decorators (`FilterStreamTypes`, `FilterEventTypes`).
  - Upgraded core dependency to `github.com/eventsalsa/store` v0.2.0.

### v0.1.0

- **Breaking Changes**:
  - Replaced `Aggregate` terminology with `Stream` across all projection contracts, event filtering, and persisted event structures.
  - Upgraded core dependency to `github.com/eventsalsa/store` v0.1.0.
- **Improvements**:
  - Migrated integration test suites to `testcontainers-go` for automated ephemeral PostgreSQL container provisioning.
  - Configured automated release management and versioning with Release Please.

### v0.0.3

- Migrated PostgreSQL projector database operations to native `pgx/v5`, removing the `database/sql` coupling.
- Introduced table-based lease leader election (`projector.LeaderStrategyLease`) to support deployments running behind PgBouncer in transaction pooling mode.
- Added a cascading delete constraint to the leader election table.
- Added driver-agnostic database serialization failure detection.
- Expanded integration test suite to cover partitioning, reconnection, and leader demotion scenarios.

### v0.0.2

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating projector infrastructure migrations.
- Documented both the quick CLI flow and the package-level migration API.

### v0.0.1

- Initial release.

## eventsalsa/encryption

### v0.1.0

- **Breaking Changes**:
  - Migrated Go module import paths to `eventsalsa.dev/encryption`, `eventsalsa.dev/encryption/kek/awskms` (`v0.0.1`), and `eventsalsa.dev/encryption/kek/gcpkms` (`v0.0.1`).
- **Improvements**:
  - Updated toolchain compatibility to Go 1.27.4 and harmonized strict static analysis across the monorepo.

### v0.0.6

- **Breaking Changes**:
  - Replaced `systemkey` with the behavior-based `kek` package (`kek.Wrapper`, `kek.Rewrapper`, `kek.Keyring`) and `kek/local` (`NewWrapper`, `NewKeyringFromMap`, `NewKeyringFromFiles`) so Key Encryption Keys never cross interface boundaries as raw byte slices.
  - Upgraded `cipher.Cipher`, `cipher/aesgcm`, and `envelope.Envelope` to bytes-first AEAD with Additional Authenticated Data (`aad []byte`) support and mandatory `KeyScope.AAD()` binding on all wrapped DEKs.
  - Updated `(*postgres.Store).CreateKey` and `(*postgres.Store).RotateKey` to return `(*encryption.EncryptedKey, error)` directly via PostgreSQL `RETURNING`, and renamed `postgres.RewrapSystemKeys` to `postgres.RewrapKeys`.
  - Refactored the PostgreSQL keystore schema (`001_encryption_keys.sql`) to rename `encrypted_key` → `encrypted_dek` and `system_key_id` → `kek_id`, enforce the single-active-key invariant via a partial unique index, and optimize the composite `kek_id` rewrap index.
- **Features**:
  - Added isolated Go sub-modules for Cloud KMS KEK providers: **AWS KMS** (`github.com/eventsalsa/encryption/kek/awskms`, with server-side `kms:ReEncrypt` support and configurable `WithContextKey`) and **Google Cloud KMS** (`github.com/eventsalsa/encryption/kek/gcpkms`, with end-to-end Castagnoli CRC32C integrity verification).
  - Introduced `envelope.Session` (`env.OpenSession(ctx, key)`) to unwrap a DEK once and perform multiple in-memory AEAD operations without repeated KMS calls.
  - Added `kek.CachingKeyring`, a concurrency-safe bounded TTL + LRU DEK cache decorator that automatically zeroes cached DEKs on expiration, eviction, invalidation, or close.

### v0.0.5

- **Breaking Changes**:
  - Decoupled envelope encryption into a pure in-memory cryptographic engine (`envelope.Envelope`) with zero database or context dependencies.
  - Refactored `postgres.Store` into a stateless key store where all operations accept an explicit `pgx` connection (`*pgxpool.Pool`, `pgx.Tx`, `*pgx.Conn`), eliminating stateful context cloning and `keystore.WithTx`.
  - Consolidated DEK lifecycle management directly into `postgres.Store` (`CreateKey`, `RotateKey`, `GetActiveKey`, `GetKey`, `RevokeKeys`, `DestroyKeys`, `ActiveKeyVersion`).
  - Converted `RewrapSystemKeys` into a standalone administrative function in `postgres`.
  - Removed domain adapter packages (`pii`, `secret`, `keymanager`, `encerr`) and `encryption.Module` in favor of direct envelope primitives, clean component injection, and root sentinel errors.

### v0.0.4

- Migrated PostgreSQL keystore implementation and transactional propagation helpers (`keystore.WithTx`) to native `pgx/v5`, removing dependencies on `database/sql`.

### v0.0.3

- Added the PostgreSQL system-key rewrap API with `(*postgres.Store).RewrapSystemKeys` for re-encrypting stored DEKs from one system key ID to another in short batches.
- Added dry-run support and progress counters so operators can preview the migration and track `rewrapped`, `remaining`, and batch counts during execution.
- Expanded the README and PostgreSQL test coverage around the recommended rewrap workflow, validation, idempotency, concurrent runs, and historical decryptability after old-key retirement.

### v0.0.2

- Added `cmd/migrate-gen`, a stable CLI entrypoint for generating or printing the PostgreSQL key-store migration.
- Added package helpers in `keystore/postgres/migrations` for rendering and writing migration SQL with schema and table overrides.
- Updated the documentation to cover the CLI-first migration flow and the package-level alternative.

### v0.0.1

- Initial release.

## eventsalsa/snapshot

### v0.0.2

- **Breaking Changes**:
  - Migrated Go module import paths to `eventsalsa.dev/snapshot` and `eventsalsa.dev/store` (`v0.3.0`).
- **Fixes**:
  - Updated PostgreSQL integration and example schemas to generate store tables via `eventsalsa.dev/store/migrations.SQL` instead of hardcoded SQL DDL.
  - Checked `gzip.Reader.Close()` errors on snapshot decompression and tightened migration directory permissions (`0o750`).

### v0.0.1

- Initial release (`github.com/eventsalsa/snapshot`).
