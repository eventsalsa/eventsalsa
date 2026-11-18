//go:build integration

package integration_test

import (
	"context"
	"fmt"
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

	all, err := pgStore.ReadEvents(ctx, readTx, 0, 10)
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

			readAll, err := pgStore.ReadEvents(ctx, rtx, 0, 10)
			if err != nil {
				t.Fatalf("ReadEvents (%s): %v", tc.name, err)
			}
			if len(readAll) != 1 || readAll[0].TransactionID != txID {
				t.Fatalf("%s: ReadEvents TransactionID mismatch: got %+v want %d", tc.name, readAll, txID)
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

			all, err := pgStore.ReadEvents(ctx, rtx, 0, 10)
			if err != nil || len(all) != 1 || all[0].TransactionID != res.Events[0].TransactionID {
				t.Fatalf("ReadEvents (%s) mismatch: all=%+v err=%v", strategy, all, err)
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
				correlation_id TEXT,
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
		events, err := pgStore.ReadEvents(ctx, rtx, 0, 10)
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
				correlation_id TEXT,
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
