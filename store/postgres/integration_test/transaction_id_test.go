//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/store"
	"eventsalsa.dev/store/migrations"
	"eventsalsa.dev/store/postgres"
)

func newEvent(streamType, streamID, eventType string) store.Event {
	return store.Event{
		StreamType:   streamType,
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    eventType,
		EventVersion: 1,
		Payload:      []byte(`{"ok":true}`),
		Metadata:     []byte(`{"source":"integration"}`),
		CreatedAt:    time.Now(),
	}
}

func TestTransactionID_SameTxSharedAndMonotonicAcrossTx(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Transaction 1 appends two separate calls on different streams inside one tx.
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx tx1: %v", err)
	}
	defer func() { _ = tx1.Rollback(ctx) }()

	var pgTx1 uint64
	if err := tx1.QueryRow(ctx, "SELECT pg_current_xact_id()").Scan(&pgTx1); err != nil {
		t.Fatalf("SELECT pg_current_xact_id tx1: %v", err)
	}

	s1 := uuid.New().String()
	s2 := uuid.New().String()

	res1, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{
		newEvent("Order", s1, "OrderCreated"),
		newEvent("Order", s1, "OrderUpdated"),
	})
	if err != nil {
		t.Fatalf("Append 1a: %v", err)
	}
	res2, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{
		newEvent("Invoice", s2, "InvoiceIssued"),
	})
	if err != nil {
		t.Fatalf("Append 1b: %v", err)
	}

	if res1.Events[0].TransactionID == 0 || res1.Events[0].TransactionID != pgTx1 {
		t.Fatalf("res1[0].TransactionID = %d, want %d", res1.Events[0].TransactionID, pgTx1)
	}
	if res1.Events[1].TransactionID != pgTx1 {
		t.Fatalf("res1[1].TransactionID = %d, want %d", res1.Events[1].TransactionID, pgTx1)
	}
	if res2.Events[0].TransactionID != pgTx1 {
		t.Fatalf("res2[0].TransactionID = %d, want %d", res2.Events[0].TransactionID, pgTx1)
	}

	// Reading within the same uncommitted transaction returns the same TransactionID.
	inTxStream, err := pgStore.ReadStream(ctx, tx1, "Order", s1, nil, nil)
	if err != nil {
		t.Fatalf("in-tx ReadStream: %v", err)
	}
	if len(inTxStream.Events) != 2 || inTxStream.Events[0].TransactionID != pgTx1 || inTxStream.Events[1].TransactionID != pgTx1 {
		t.Fatalf("unexpected in-tx stream TransactionIDs: %+v", inTxStream.Events)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// Transaction 2 must have a strictly greater TransactionID.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()

	s3 := uuid.New().String()
	res3, err := pgStore.Append(ctx, tx2, store.NoStream(), []store.Event{
		newEvent("Order", s3, "OrderCompleted"),
	})
	if err != nil {
		t.Fatalf("Append tx2: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit tx2: %v", err)
	}

	if res3.Events[0].TransactionID <= pgTx1 {
		t.Fatalf("tx2 TransactionID %d <= tx1 TransactionID %d", res3.Events[0].TransactionID, pgTx1)
	}

	// ReadEvents & ReadStream parity after commit.
	readTx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx readTx: %v", err)
	}
	defer func() { _ = readTx.Rollback(ctx) }()

	all, err := pgStore.ReadEvents(ctx, readTx, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("len(all) = %d, want 4", len(all))
	}
	if all[0].TransactionID != pgTx1 || all[1].TransactionID != pgTx1 || all[2].TransactionID != pgTx1 {
		t.Fatalf("unexpected tx1 ReadEvents TransactionIDs: %d, %d, %d", all[0].TransactionID, all[1].TransactionID, all[2].TransactionID)
	}
	if all[3].TransactionID != res3.Events[0].TransactionID {
		t.Fatalf("ReadEvents[3].TransactionID = %d, want %d", all[3].TransactionID, res3.Events[0].TransactionID)
	}

	stream3, err := pgStore.ReadStream(ctx, readTx, "Order", s3, nil, nil)
	if err != nil {
		t.Fatalf("ReadStream s3: %v", err)
	}
	if len(stream3.Events) != 1 || stream3.Events[0].TransactionID != res3.Events[0].TransactionID {
		t.Fatalf("ReadStream s3 mismatch: %+v", stream3.Events)
	}
}

func TestTransactionID_AllQueryExecModes(t *testing.T) {
	modes := []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"CacheStatement", pgx.QueryExecModeCacheStatement},
		{"CacheDescribe", pgx.QueryExecModeCacheDescribe},
		{"DescribeExec", pgx.QueryExecModeDescribeExec},
		{"Exec", pgx.QueryExecModeExec},
		{"SimpleProtocol", pgx.QueryExecModeSimpleProtocol},
	}

	ctx := context.Background()
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := pgxpool.ParseConfig(testDBConnStr)
			if err != nil {
				t.Fatalf("ParseConfig: %v", err)
			}
			cfg.ConnConfig.DefaultQueryExecMode = tc.mode

			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatalf("NewWithConfig: %v", err)
			}
			defer pool.Close()

			db := testDB{pool}
			setupTestTables(t, db)
			pgStore := postgres.NewStore(postgres.DefaultStoreConfig())
			streamID := uuid.New().String()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			res, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
				newEvent("ModeTest", streamID, "ModeTested"),
			})
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("Append (%s): %v", tc.name, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit: %v", err)
			}

			txID := res.Events[0].TransactionID
			if txID == 0 {
				t.Fatalf("%s: Append TransactionID is 0", tc.name)
			}

			rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("BeginTx read: %v", err)
			}
			defer func() { _ = rtx.Rollback(ctx) }()

			stream, err := pgStore.ReadStream(ctx, rtx, "ModeTest", streamID, nil, nil)
			if err != nil {
				t.Fatalf("ReadStream (%s): %v", tc.name, err)
			}
			if len(stream.Events) != 1 || stream.Events[0].TransactionID != txID {
				t.Fatalf("%s: ReadStream TransactionID mismatch: got %+v want %d", tc.name, stream.Events, txID)
			}

			readAll, err := pgStore.ReadEvents(ctx, rtx, store.Cursor{}, 10)
			if err != nil {
				t.Fatalf("ReadEvents (%s): %v", tc.name, err)
			}
			if len(readAll) != 1 || readAll[0].TransactionID != txID {
				t.Fatalf("%s: ReadEvents TransactionID mismatch: got %+v want %d", tc.name, readAll, txID)
			}

			head, err := pgStore.GetHead(ctx, rtx)
			if err != nil {
				t.Fatalf("GetHead (%s): %v", tc.name, err)
			}
			wantHead := store.Cursor{TransactionID: txID, GlobalPosition: readAll[0].GlobalPosition}
			if head != wantHead {
				t.Fatalf("%s: GetHead mismatch: got %+v want %+v", tc.name, head, wantHead)
			}
		})
	}
}

func TestTransactionID_NativePartitioningAndSchemaQualifiedMigrations(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	ctx := context.Background()

	for _, strategy := range []migrations.PartitionStrategy{
		migrations.PartitionStrategyNone,
		migrations.PartitionStrategyNative,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			if _, err := db.Exec(ctx, "DROP SCHEMA IF EXISTS ordering CASCADE"); err != nil {
				t.Fatalf("drop ordering schema: %v", err)
			}
			t.Cleanup(func() {
				_, _ = db.Exec(context.Background(), "DROP SCHEMA IF EXISTS ordering CASCADE")
			})

			ddl, err := migrations.SQL(&migrations.Config{
				EventsTable:      "ordering.events",
				StreamHeadsTable: "ordering.stream_heads",
				Partitioning: migrations.PartitionConfig{
					Strategy:          strategy,
					PartitionSize:     1000,
					InitialPartitions: 2,
				},
			})
			if err != nil {
				t.Fatalf("migrations.SQL: %v", err)
			}
			if _, err := db.Exec(ctx, ddl); err != nil {
				t.Fatalf("execute schema-qualified migration (%s): %v", strategy, err)
			}

			var idxCount int
			err = db.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM pg_indexes
				WHERE schemaname = 'ordering'
				  AND tablename = 'events'
				  AND indexname = 'idx_events_transaction_position'
			`).Scan(&idxCount)
			if err != nil || idxCount != 1 {
				t.Fatalf("idx_events_transaction_position check failed: count=%d err=%v", idxCount, err)
			}

			pgStore := postgres.NewStore(postgres.NewStoreConfig(
				postgres.WithEventsTable("ordering.events"),
				postgres.WithStreamHeadsTable("ordering.stream_heads"),
			))

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			res, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
				newEvent("Ordering", uuid.New().String(), "Placed"),
			})
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("Append (%s): %v", strategy, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if res.Events[0].TransactionID == 0 {
				t.Fatalf("%s: expected non-zero TransactionID", strategy)
			}

			rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("BeginTx read: %v", err)
			}
			defer func() { _ = rtx.Rollback(ctx) }()

			all, err := pgStore.ReadEvents(ctx, rtx, store.Cursor{}, 10)
			if err != nil || len(all) != 1 || all[0].TransactionID != res.Events[0].TransactionID {
				t.Fatalf("ReadEvents (%s) mismatch: all=%+v err=%v", strategy, all, err)
			}
			head, err := pgStore.GetHead(ctx, rtx)
			wantHead := store.Cursor{TransactionID: res.Events[0].TransactionID, GlobalPosition: res.Events[0].GlobalPosition}
			if err != nil || head != wantHead {
				t.Fatalf("GetHead (%s) mismatch: got=%+v want=%+v err=%v", strategy, head, wantHead, err)
			}
		})
	}
}

func TestUpgradeSQL_PreV03ToV03(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	ctx := context.Background()

	t.Run("unpartitioned_online_upgrade", func(t *testing.T) {
		schema := fmt.Sprintf("upg_unpart_%d", time.Now().UnixNano()%1000000)
		_, err := db.Exec(ctx, fmt.Sprintf(`
			DROP SCHEMA IF EXISTS %s CASCADE;
			CREATE SCHEMA %s;
			CREATE TABLE %s.events (
				global_position BIGSERIAL PRIMARY KEY,
				stream_type TEXT NOT NULL,
				stream_id TEXT NOT NULL,
				stream_version BIGINT NOT NULL,
				event_id UUID NOT NULL UNIQUE,
				event_type TEXT NOT NULL,
				event_version INT NOT NULL DEFAULT 1,
				payload BYTEA NOT NULL,
				trace_id TEXT,
				correlation_id TEXT, -- retained in legacy fixture to verify pre-v0.5.0 zero-downtime compatibility
				causation_id TEXT,
				metadata JSONB,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				UNIQUE (stream_type, stream_id, stream_version)
			);
			CREATE TABLE %s.stream_heads (
				stream_type TEXT NOT NULL,
				stream_id TEXT NOT NULL,
				stream_version BIGINT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (stream_type, stream_id)
			);
			INSERT INTO %s.events (stream_type, stream_id, stream_version, event_id, event_type, payload)
			VALUES ('Order', 'legacy-1', 1, gen_random_uuid(), 'LegacyEvent', '\x7b7d');
		`, schema, schema, schema, schema, schema))
		if err != nil {
			t.Fatalf("setup legacy unpartitioned table: %v", err)
		}
		defer func() { _, _ = db.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)) }()

		steps := []string{
			fmt.Sprintf("ALTER TABLE %s.events ADD COLUMN IF NOT EXISTS transaction_id xid8", schema),
			fmt.Sprintf("ALTER TABLE %s.events ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()", schema),
			fmt.Sprintf(`WITH batch AS (
				SELECT global_position FROM %s.events WHERE transaction_id IS NULL ORDER BY global_position LIMIT 50000
			) UPDATE %s.events e SET transaction_id = '0'::xid8 FROM batch WHERE e.global_position = batch.global_position`, schema, schema),
			fmt.Sprintf("ALTER TABLE %s.events ADD CONSTRAINT events_transaction_id_not_null CHECK (transaction_id IS NOT NULL) NOT VALID", schema),
			fmt.Sprintf("ALTER TABLE %s.events VALIDATE CONSTRAINT events_transaction_id_not_null", schema),
			fmt.Sprintf("ALTER TABLE %s.events ALTER COLUMN transaction_id SET NOT NULL", schema),
			fmt.Sprintf("ALTER TABLE %s.events DROP CONSTRAINT events_transaction_id_not_null", schema),
			fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_transaction_position ON %s.events (transaction_id, global_position)", schema),
		}
		for i, stmt := range steps {
			if _, err := db.Exec(ctx, stmt); err != nil {
				t.Fatalf("unpartitioned upgrade step %d failed (%s): %v", i, stmt, err)
			}
		}

		pgStore := postgres.NewStore(postgres.NewStoreConfig(
			postgres.WithEventsTable(schema+".events"),
			postgres.WithStreamHeadsTable(schema+".stream_heads"),
		))
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		res, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
			newEvent("Order", "new-1", "NewEvent"),
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("Append post-upgrade: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit post-upgrade: %v", err)
		}
		if res.Events[0].TransactionID == 0 {
			t.Fatal("expected non-zero TransactionID on new event")
		}

		rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
		if err != nil {
			t.Fatalf("BeginTx read: %v", err)
		}
		defer func() { _ = rtx.Rollback(ctx) }()
		events, err := pgStore.ReadEvents(ctx, rtx, store.Cursor{}, 10)
		if err != nil || len(events) != 2 {
			t.Fatalf("ReadEvents post-upgrade: len=%d err=%v", len(events), err)
		}
		if events[0].TransactionID != 0 {
			t.Fatalf("legacy event TransactionID = %d, want 0", events[0].TransactionID)
		}
		if events[1].TransactionID != res.Events[0].TransactionID {
			t.Fatalf("new event TransactionID = %d, want %d", events[1].TransactionID, res.Events[0].TransactionID)
		}
	})

	t.Run("partitioned_online_upgrade", func(t *testing.T) {
		schema := fmt.Sprintf("upg_part_%d", time.Now().UnixNano()%1000000)
		_, err := db.Exec(ctx, fmt.Sprintf(`
			DROP SCHEMA IF EXISTS %s CASCADE;
			CREATE SCHEMA %s;
			CREATE SEQUENCE %s.events_global_position_seq;
			CREATE TABLE %s.events (
				global_position BIGINT NOT NULL DEFAULT nextval('%s.events_global_position_seq'),
				stream_type TEXT NOT NULL,
				stream_id TEXT NOT NULL,
				stream_version BIGINT NOT NULL,
				event_id UUID NOT NULL,
				event_type TEXT NOT NULL,
				event_version INT NOT NULL DEFAULT 1,
				payload BYTEA NOT NULL,
				trace_id TEXT,
				correlation_id TEXT, -- retained in legacy fixture to verify pre-v0.5.0 zero-downtime compatibility
				causation_id TEXT,
				metadata JSONB,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (global_position)
			) PARTITION BY RANGE (global_position);
			CREATE TABLE %s.events_p0000000001_p0000001000 PARTITION OF %s.events FOR VALUES FROM (1) TO (1001);
			CREATE TABLE %s.stream_heads (
				stream_type TEXT NOT NULL,
				stream_id TEXT NOT NULL,
				stream_version BIGINT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				PRIMARY KEY (stream_type, stream_id)
			);
			INSERT INTO %s.events (stream_type, stream_id, stream_version, event_id, event_type, payload)
			VALUES ('Order', 'legacy-p1', 1, gen_random_uuid(), 'LegacyEvent', '\x7b7d');
		`, schema, schema, schema, schema, schema, schema, schema, schema, schema))
		if err != nil {
			t.Fatalf("setup legacy partitioned table: %v", err)
		}
		defer func() { _, _ = db.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema)) }()

		steps := []string{
			fmt.Sprintf("ALTER TABLE %s.events ADD COLUMN IF NOT EXISTS transaction_id xid8", schema),
			fmt.Sprintf("ALTER TABLE %s.events ALTER COLUMN transaction_id SET DEFAULT pg_current_xact_id()", schema),
			fmt.Sprintf("UPDATE %s.events SET transaction_id = '0'::xid8 WHERE transaction_id IS NULL", schema),
			fmt.Sprintf("ALTER TABLE %s.events ADD CONSTRAINT events_transaction_id_not_null CHECK (transaction_id IS NOT NULL) NOT VALID", schema),
			fmt.Sprintf("ALTER TABLE %s.events VALIDATE CONSTRAINT events_transaction_id_not_null", schema),
			fmt.Sprintf("ALTER TABLE %s.events ALTER COLUMN transaction_id SET NOT NULL", schema),
			fmt.Sprintf("ALTER TABLE %s.events DROP CONSTRAINT events_transaction_id_not_null", schema),
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_events_transaction_position ON ONLY %s.events (transaction_id, global_position)", schema),
			fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_p0000000001_p0000001000_transaction_position ON %s.events_p0000000001_p0000001000 (transaction_id, global_position)", schema),
			fmt.Sprintf("ALTER INDEX %s.idx_events_transaction_position ATTACH PARTITION %s.idx_events_p0000000001_p0000001000_transaction_position", schema, schema),
		}
		for i, stmt := range steps {
			if _, err := db.Exec(ctx, stmt); err != nil {
				t.Fatalf("partitioned upgrade step %d failed (%s): %v", i, stmt, err)
			}
		}

		pgStore := postgres.NewStore(postgres.NewStoreConfig(
			postgres.WithEventsTable(schema+".events"),
			postgres.WithStreamHeadsTable(schema+".stream_heads"),
		))
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		res, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
			newEvent("Order", "new-p1", "NewEvent"),
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("Append partitioned post-upgrade: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit partitioned post-upgrade: %v", err)
		}
		if res.Events[0].TransactionID == 0 {
			t.Fatal("expected non-zero TransactionID on new partitioned event")
		}
	})
}

func TestMVCC_UncommittedOlderTxBlocksNewerCommit(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	resA, err := pgStore.Append(ctx, txA, store.NoStream(), []store.Event{
		newEvent("Order", "sA", "EventA"),
	})
	if err != nil {
		t.Fatalf("Append A: %v", err)
	}

	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx B: %v", err)
	}
	defer func() { _ = txB.Rollback(ctx) }()

	resB, err := pgStore.Append(ctx, txB, store.NoStream(), []store.Event{
		newEvent("Order", "sB", "EventB"),
	})
	if err != nil {
		t.Fatalf("Append B: %v", err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("Commit B: %v", err)
	}

	rtx1, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read1: %v", err)
	}
	defer func() { _ = rtx1.Rollback(ctx) }()

	rows1, err := pgStore.ReadEvents(ctx, rtx1, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents while blocked: %v", err)
	}
	if len(rows1) != 0 {
		t.Fatalf("ReadEvents while blocked len = %d, want 0", len(rows1))
	}
	head1, err := pgStore.GetHead(ctx, rtx1)
	if err != nil {
		t.Fatalf("GetHead while blocked: %v", err)
	}
	if !head1.IsZero() {
		t.Fatalf("GetHead while blocked = %+v, want zero", head1)
	}
	_ = rtx1.Rollback(ctx)

	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("Commit A: %v", err)
	}

	rtx2, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read2: %v", err)
	}
	defer func() { _ = rtx2.Rollback(ctx) }()

	rows2, err := pgStore.ReadEvents(ctx, rtx2, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents after A commits: %v", err)
	}
	if len(rows2) != 2 {
		t.Fatalf("len = %d, want 2", len(rows2))
	}
	if rows2[0].GlobalPosition != resA.GlobalPositions[0] || rows2[1].GlobalPosition != resB.GlobalPositions[0] {
		t.Fatalf("unexpected positions [%d, %d], want [%d, %d]",
			rows2[0].GlobalPosition, rows2[1].GlobalPosition, resA.GlobalPositions[0], resB.GlobalPositions[0])
	}
}

func TestMVCC_AbortedOlderTxUnblocksImmediately(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	if _, err := pgStore.Append(ctx, txA, store.NoStream(), []store.Event{
		newEvent("Order", "sA", "AbortedEvent"),
	}); err != nil {
		t.Fatalf("Append A: %v", err)
	}

	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx B: %v", err)
	}
	resB, err := pgStore.Append(ctx, txB, store.NoStream(), []store.Event{
		newEvent("Order", "sB", "CommittedEvent"),
	})
	if err != nil {
		_ = txB.Rollback(ctx)
		t.Fatalf("Append B: %v", err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("Commit B: %v", err)
	}

	if err := txA.Rollback(ctx); err != nil {
		t.Fatalf("Rollback A: %v", err)
	}

	rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read: %v", err)
	}
	defer func() { _ = rtx.Rollback(ctx) }()

	rows, err := pgStore.ReadEvents(ctx, rtx, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(rows) != 1 || rows[0].GlobalPosition != resB.GlobalPositions[0] {
		t.Fatalf("rows = %+v, want single event at pos %d", rows, resB.GlobalPositions[0])
	}
}

func TestMVCC_ReverseSequenceVsXIDOrder_NoSkippedEvents(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// txA acquires an older XID first, before allocating a sequence position.
	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx A: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	var xidA uint64
	if err := txA.QueryRow(ctx, "SELECT pg_current_xact_id()").Scan(&xidA); err != nil {
		t.Fatalf("acquire xidA: %v", err)
	}

	// txB gets a newer XID (xidB > xidA) AND allocates an earlier sequence value (pos 1).
	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx B: %v", err)
	}
	defer func() { _ = txB.Rollback(ctx) }()

	resB, err := pgStore.Append(ctx, txB, store.NoStream(), []store.Event{
		newEvent("Order", "sB", "NewerXIDEarlierPos"),
	})
	if err != nil {
		t.Fatalf("Append B: %v", err)
	}

	// txA now appends with older XID and higher sequence value (pos 2), and commits first.
	resA, err := pgStore.Append(ctx, txA, store.NoStream(), []store.Event{
		newEvent("Order", "sA", "OlderXIDHigherPos"),
	})
	if err != nil {
		t.Fatalf("Append A: %v", err)
	}
	if resA.Events[0].TransactionID >= resB.Events[0].TransactionID {
		t.Fatalf("expected xidA < xidB, got %d >= %d", resA.Events[0].TransactionID, resB.Events[0].TransactionID)
	}
	if resA.GlobalPositions[0] <= resB.GlobalPositions[0] {
		t.Fatalf("expected posA > posB, got %d <= %d", resA.GlobalPositions[0], resB.GlobalPositions[0])
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("Commit A: %v", err)
	}

	// While txB is still open, txA (< xmin) is settled and delivered first.
	rtx1, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read1: %v", err)
	}
	defer func() { _ = rtx1.Rollback(ctx) }()

	batch1, err := pgStore.ReadEvents(ctx, rtx1, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents batch1: %v", err)
	}
	if len(batch1) != 1 || batch1[0].GlobalPosition != resA.GlobalPositions[0] {
		t.Fatalf("batch1 = %+v, want only txA event at pos %d", batch1, resA.GlobalPositions[0])
	}
	cursorAfterA := store.Cursor{
		TransactionID:  batch1[0].TransactionID,
		GlobalPosition: batch1[0].GlobalPosition,
	}
	head1, err := pgStore.GetHead(ctx, rtx1)
	if err != nil || head1 != cursorAfterA {
		t.Fatalf("head1 = %+v (err=%v), want %+v", head1, err, cursorAfterA)
	}
	_ = rtx1.Rollback(ctx)

	// After txB commits, (xidB, posB) > (xidA, posA) must still deliver txB even though posB < posA.
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("Commit B: %v", err)
	}

	rtx2, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read2: %v", err)
	}
	defer func() { _ = rtx2.Rollback(ctx) }()

	batch2, err := pgStore.ReadEvents(ctx, rtx2, cursorAfterA, 10)
	if err != nil {
		t.Fatalf("ReadEvents batch2: %v", err)
	}
	if len(batch2) != 1 || batch2[0].GlobalPosition != resB.GlobalPositions[0] {
		t.Fatalf("batch2 = %+v, want txB event at pos %d (must not be skipped)", batch2, resB.GlobalPositions[0])
	}
	head2, err := pgStore.GetHead(ctx, rtx2)
	if err != nil {
		t.Fatalf("GetHead after B commits: %v", err)
	}
	wantHead2 := store.Cursor{
		TransactionID:  resB.Events[0].TransactionID,
		GlobalPosition: resB.GlobalPositions[0],
	}
	if head2 != wantHead2 {
		t.Fatalf("head2 = %+v, want %+v", head2, wantHead2)
	}
}

func TestMVCC_MultiEventTransactionPaginationAcrossBatches(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	res1, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
		newEvent("Order", "s1", "E1"),
		newEvent("Order", "s1", "E2"),
		newEvent("Order", "s1", "E3"),
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Append s1: %v", err)
	}
	res2, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
		newEvent("Invoice", "s2", "E4"),
		newEvent("Invoice", "s2", "E5"),
	})
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Append s2: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if res1.Events[0].TransactionID != res2.Events[1].TransactionID {
		t.Fatalf("expected shared TransactionID in same tx: %d vs %d",
			res1.Events[0].TransactionID, res2.Events[1].TransactionID)
	}

	rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read: %v", err)
	}
	defer func() { _ = rtx.Rollback(ctx) }()

	var cursor store.Cursor
	var collected []store.PersistedEvent
	for {
		batch, err := pgStore.ReadEvents(ctx, rtx, cursor, 2)
		if err != nil {
			t.Fatalf("ReadEvents cursor %+v: %v", cursor, err)
		}
		if len(batch) == 0 {
			break
		}
		collected = append(collected, batch...)
		last := batch[len(batch)-1]
		cursor = store.Cursor{TransactionID: last.TransactionID, GlobalPosition: last.GlobalPosition}
	}

	if len(collected) != 5 {
		t.Fatalf("collected %d events, want 5", len(collected))
	}
	for i, e := range collected {
		if e.GlobalPosition != int64(i+1) {
			t.Fatalf("collected[%d].GlobalPosition = %d, want %d", i, e.GlobalPosition, i+1)
		}
	}
}

func TestMVCC_SavepointRollbackAndSubtransactions(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// 1. Rolled-back savepoint never leaks and never stalls subsequent commits.
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx 1: %v", err)
	}
	if _, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{
		newEvent("Order", "sp-a", "Keep1"),
	}); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("Append Keep1: %v", err)
	}
	if _, err := tx1.Exec(ctx, "SAVEPOINT sp_drop"); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("SAVEPOINT sp_drop: %v", err)
	}
	if _, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{
		newEvent("Order", "sp-drop", "Dropped"),
	}); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("Append Dropped: %v", err)
	}
	if _, err := tx1.Exec(ctx, "ROLLBACK TO SAVEPOINT sp_drop"); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("ROLLBACK TO SAVEPOINT sp_drop: %v", err)
	}
	if _, err := pgStore.Append(ctx, tx1, store.Exact(1), []store.Event{
		newEvent("Order", "sp-a", "Keep2"),
	}); err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("Append Keep2: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit tx1: %v", err)
	}

	// 2. Interleaved parent XID < txConcurrent < committed savepoint sub-XID.
	txParent, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx parent: %v", err)
	}
	defer func() { _ = txParent.Rollback(ctx) }()
	if _, err := pgStore.Append(ctx, txParent, store.NoStream(), []store.Event{
		newEvent("Order", "sp-parent", "ParentEvent"),
	}); err != nil {
		t.Fatalf("Append ParentEvent: %v", err)
	}

	txConcurrent, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx concurrent: %v", err)
	}
	if _, err := pgStore.Append(ctx, txConcurrent, store.NoStream(), []store.Event{
		newEvent("Order", "sp-conc", "ConcurrentEvent"),
	}); err != nil {
		_ = txConcurrent.Rollback(ctx)
		t.Fatalf("Append ConcurrentEvent: %v", err)
	}

	if _, err := txParent.Exec(ctx, "SAVEPOINT sp_keep"); err != nil {
		_ = txConcurrent.Rollback(ctx)
		t.Fatalf("SAVEPOINT sp_keep: %v", err)
	}
	if _, err := pgStore.Append(ctx, txParent, store.NoStream(), []store.Event{
		newEvent("Order", "sp-child", "ChildEvent"),
	}); err != nil {
		_ = txConcurrent.Rollback(ctx)
		t.Fatalf("Append ChildEvent: %v", err)
	}
	if _, err := txParent.Exec(ctx, "RELEASE SAVEPOINT sp_keep"); err != nil {
		_ = txConcurrent.Rollback(ctx)
		t.Fatalf("RELEASE SAVEPOINT sp_keep: %v", err)
	}

	// Concurrent tx commits while txParent (with subtransaction) is still open.
	if err := txConcurrent.Commit(ctx); err != nil {
		t.Fatalf("Commit txConcurrent: %v", err)
	}

	rtxBlocked, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read blocked: %v", err)
	}
	blockedEvents, err := pgStore.ReadEvents(ctx, rtxBlocked, store.Cursor{}, 10)
	_ = rtxBlocked.Rollback(ctx)
	if err != nil {
		t.Fatalf("ReadEvents blocked: %v", err)
	}
	// Only tx1's 2 events should be visible while txParent is open.
	if len(blockedEvents) != 2 {
		t.Fatalf("blocked len = %d, want 2 (ParentEvent, ChildEvent, ConcurrentEvent must wait)", len(blockedEvents))
	}

	if err := txParent.Commit(ctx); err != nil {
		t.Fatalf("Commit txParent: %v", err)
	}

	rtxFinal, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read final: %v", err)
	}
	defer func() { _ = rtxFinal.Rollback(ctx) }()

	var cursor store.Cursor
	var types []string
	for {
		batch, err := pgStore.ReadEvents(ctx, rtxFinal, cursor, 1)
		if err != nil {
			t.Fatalf("ReadEvents limit=1: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, ev := range batch {
			types = append(types, ev.EventType)
		}
		last := batch[len(batch)-1]
		cursor = store.Cursor{TransactionID: last.TransactionID, GlobalPosition: last.GlobalPosition}
	}

	if len(types) != 5 {
		t.Fatalf("delivered %d events %v, want 5 committed events without Dropped", len(types), types)
	}
	for _, et := range types {
		if et == "Dropped" {
			t.Fatalf("rolled-back savepoint event leaked: %v", types)
		}
	}
}

func TestMVCC_BackfilledZeroXIDToLiveXIDTransition(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	for i := 1; i <= 3; i++ {
		if _, err := db.Exec(ctx, `
			INSERT INTO events (transaction_id, stream_type, stream_id, stream_version, event_id, event_type, payload)
			VALUES ('0'::xid8, 'Legacy', $1, 1, gen_random_uuid(), $2, '\x7b7d')
		`, fmt.Sprintf("leg-%d", i), fmt.Sprintf("Legacy%d", i)); err != nil {
			t.Fatalf("insert legacy row %d: %v", i, err)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := pgStore.Append(ctx, tx, store.NoStream(), []store.Event{
		newEvent("Live", "live-1", "Live4"),
		newEvent("Live", "live-1", "Live5"),
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("Append live events: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rtx, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read: %v", err)
	}
	defer func() { _ = rtx.Rollback(ctx) }()

	var cursor store.Cursor
	var all []store.PersistedEvent
	for {
		batch, err := pgStore.ReadEvents(ctx, rtx, cursor, 2)
		if err != nil {
			t.Fatalf("ReadEvents cursor %+v: %v", cursor, err)
		}
		if len(batch) == 0 {
			break
		}
		all = append(all, batch...)
		last := batch[len(batch)-1]
		cursor = store.Cursor{TransactionID: last.TransactionID, GlobalPosition: last.GlobalPosition}
	}

	if len(all) != 5 {
		t.Fatalf("len(all) = %d, want 5", len(all))
	}
	for i := 0; i < 3; i++ {
		if all[i].TransactionID != 0 || all[i].GlobalPosition != int64(i+1) {
			t.Fatalf("legacy row %d mismatch: %+v", i, all[i])
		}
	}
	for i := 3; i < 5; i++ {
		if all[i].TransactionID == 0 || all[i].GlobalPosition != int64(i+1) {
			t.Fatalf("live row %d mismatch: %+v", i, all[i])
		}
	}

	head, err := pgStore.GetHead(ctx, rtx)
	if err != nil {
		t.Fatalf("GetHead: %v", err)
	}
	wantHead := store.Cursor{TransactionID: all[4].TransactionID, GlobalPosition: all[4].GlobalPosition}
	if head != wantHead {
		t.Fatalf("GetHead = %+v, want %+v", head, wantHead)
	}
}

func TestMVCC_UnrelatedTransactions_ReadOnlyVsWriteXID(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS unrelated_work (id INT)`); err != nil {
		t.Fatalf("create unrelated_work: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `DROP TABLE IF EXISTS unrelated_work`)
	})

	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Seed initial settled event e1.
	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx 1: %v", err)
	}
	res1, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{
		newEvent("Order", "s1", "E1"),
	})
	if err != nil {
		_ = tx1.Rollback(ctx)
		t.Fatalf("Append 1: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Commit 1: %v", err)
	}
	cursor1 := store.Cursor{TransactionID: res1.Events[0].TransactionID, GlobalPosition: res1.GlobalPositions[0]}

	// 1. Long-running read-only SELECT holds no real XID and does NOT block e2.
	txRO, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx RO: %v", err)
	}
	defer func() { _ = txRO.Rollback(ctx) }()
	var cnt int
	if err := txRO.QueryRow(ctx, `SELECT COUNT(*) FROM unrelated_work`).Scan(&cnt); err != nil {
		t.Fatalf("SELECT unrelated_work: %v", err)
	}

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx 2: %v", err)
	}
	res2, err := pgStore.Append(ctx, tx2, store.NoStream(), []store.Event{
		newEvent("Order", "s2", "E2"),
	})
	if err != nil {
		_ = tx2.Rollback(ctx)
		t.Fatalf("Append 2: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit 2: %v", err)
	}

	rtx1, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read1: %v", err)
	}
	batchRO, err := pgStore.ReadEvents(ctx, rtx1, cursor1, 10)
	_ = rtx1.Rollback(ctx)
	if err != nil {
		t.Fatalf("ReadEvents during RO: %v", err)
	}
	if len(batchRO) != 1 || batchRO[0].GlobalPosition != res2.GlobalPositions[0] {
		t.Fatalf("expected e2 visible immediately during read-only tx, got %+v", batchRO)
	}
	_ = txRO.Rollback(ctx)
	cursor2 := store.Cursor{TransactionID: res2.Events[0].TransactionID, GlobalPosition: res2.GlobalPositions[0]}

	// 2. Open write tx on unrelated_work allocates an XID and pins database-wide xmin until ended.
	txUnrelatedWrite, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx unrelated write: %v", err)
	}
	defer func() { _ = txUnrelatedWrite.Rollback(ctx) }()
	if _, err := txUnrelatedWrite.Exec(ctx, `INSERT INTO unrelated_work VALUES (1)`); err != nil {
		t.Fatalf("INSERT unrelated_work: %v", err)
	}

	tx3, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx 3: %v", err)
	}
	res3, err := pgStore.Append(ctx, tx3, store.NoStream(), []store.Event{
		newEvent("Order", "s3", "E3"),
	})
	if err != nil {
		_ = tx3.Rollback(ctx)
		t.Fatalf("Append 3: %v", err)
	}
	if err := tx3.Commit(ctx); err != nil {
		t.Fatalf("Commit 3: %v", err)
	}

	rtx2, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read2: %v", err)
	}
	blocked, err := pgStore.ReadEvents(ctx, rtx2, cursor2, 10)
	headBlocked, errHead := pgStore.GetHead(ctx, rtx2)
	_ = rtx2.Rollback(ctx)
	if err != nil || errHead != nil {
		t.Fatalf("ReadEvents/GetHead while unrelated write open: err=%v headErr=%v", err, errHead)
	}
	if len(blocked) != 0 || headBlocked != cursor2 {
		t.Fatalf("expected e3 held behind unrelated write xmin; got len=%d head=%+v want=%+v", len(blocked), headBlocked, cursor2)
	}

	if err := txUnrelatedWrite.Rollback(ctx); err != nil {
		t.Fatalf("Rollback unrelated write: %v", err)
	}

	rtx3, err := db.BeginTx(ctx, &txOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx read3: %v", err)
	}
	defer func() { _ = rtx3.Rollback(ctx) }()
	unblocked, err := pgStore.ReadEvents(ctx, rtx3, cursor2, 10)
	if err != nil {
		t.Fatalf("ReadEvents after unrelated rollback: %v", err)
	}
	if len(unblocked) != 1 || unblocked[0].GlobalPosition != res3.GlobalPositions[0] {
		t.Fatalf("expected e3 unblocked immediately, got %+v", unblocked)
	}
}

func TestMVCC_SimpleProtocolVisibilityAndCursorMetadata(t *testing.T) {
	db := openSimpleProtocolTestDB(t)
	ctx := context.Background()
	es := postgres.NewStore(postgres.DefaultStoreConfig())

	// Start in-flight uncommitted transaction holding an older xid before later committed appends.
	txUncommitted, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin uncommitted: %v", err)
	}
	uncommittedID := uuid.New()
	if _, appendErr := es.Append(ctx, txUncommitted, store.NoStream(), []store.Event{
		makeSimpleEvent("mvcc-uncommitted", "FirstXID", uncommittedID),
	}); appendErr != nil {
		t.Fatalf("append uncommitted: %v", appendErr)
	}

	// Commit another transaction while txUncommitted remains open.
	txCommitted, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin committed: %v", err)
	}
	committedID := uuid.New()
	if _, appendErr := es.Append(ctx, txCommitted, store.NoStream(), []store.Event{
		makeSimpleEvent("mvcc-committed", "SecondXID", committedID),
	}); appendErr != nil {
		t.Fatalf("append committed: %v", appendErr)
	}
	if commitErr := txCommitted.Commit(ctx); commitErr != nil {
		t.Fatalf("commit txCommitted: %v", commitErr)
	}

	// While txUncommitted is still open behind pg_snapshot_xmin, ReadEvents must not skip past it.
	txRead1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin read1: %v", err)
	}
	beforeCommit, err := es.ReadEvents(ctx, txRead1, store.Cursor{}, 10)
	if err != nil {
		t.Fatalf("ReadEvents while open: %v", err)
	}
	if rollbackErr := txRead1.Rollback(ctx); rollbackErr != nil {
		t.Fatalf("rollback read1: %v", rollbackErr)
	}
	if len(beforeCommit) != 0 {
		t.Fatalf("expected 0 visible events while earlier xid in-flight, got %+v", beforeCommit)
	}

	// Commit txUncommitted and paginate with cursor limit 1 -> preserves order and causal metadata.
	if commitErr := txUncommitted.Commit(ctx); commitErr != nil {
		t.Fatalf("commit uncommitted: %v", commitErr)
	}

	txRead2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin read2: %v", err)
	}
	defer func() {
		if rbErr := txRead2.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			t.Errorf("rollback read2: %v", rbErr)
		}
	}()

	page1, err := es.ReadEvents(ctx, txRead2, store.Cursor{}, 1)
	if err != nil || len(page1) != 1 {
		t.Fatalf("page1 = (%d, %v), want 1 event", len(page1), err)
	}
	cur1 := store.Cursor{TransactionID: page1[0].TransactionID, GlobalPosition: page1[0].GlobalPosition}
	page2, err := es.ReadEvents(ctx, txRead2, cur1, 1)
	if err != nil || len(page2) != 1 {
		t.Fatalf("page2 = (%d, %v), want 1 event", len(page2), err)
	}
	seenIDs := []uuid.UUID{page1[0].EventID, page2[0].EventID}
	if !slices.Equal(seenIDs, []uuid.UUID{uncommittedID, committedID}) {
		t.Fatalf("cursor order = %v, want [%s %s]", seenIDs, uncommittedID, committedID)
	}
	if page1[0].TraceID.String != "trace-mvcc-uncommitted" || page2[0].CausationID.String != "cause-mvcc-committed" {
		t.Fatalf("causal metadata lost: p1=%+v p2=%+v", page1[0], page2[0])
	}
}
