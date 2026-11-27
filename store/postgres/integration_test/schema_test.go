//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"eventsalsa.dev/store/migrations"
)

func TestMigrations_TraceIndexAndNoCorrelationColumn(t *testing.T) {
	db := openSimpleProtocolTestDB(t)
	ctx := context.Background()

	checkLiveStrategy := func(schema string, strategy migrations.PartitionStrategy) {
		if _, err := db.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE; CREATE SCHEMA %s;", schema, schema)); err != nil {
			t.Fatalf("reset schema %s: %v", schema, err)
		}
		ddl, err := migrations.SQL(&migrations.Config{
			EventsTable:      schema + ".events",
			StreamHeadsTable: schema + ".stream_heads",
			Partitioning: migrations.PartitionConfig{
				Strategy:          strategy,
				PartitionSize:     1000,
				InitialPartitions: 2,
			},
		})
		if err != nil {
			t.Fatalf("migrations.SQL(%s): %v", strategy, err)
		}
		if _, err := db.Exec(ctx, ddl); err != nil {
			t.Fatalf("apply %s DDL: %v", strategy, err)
		}

		var idxDef string
		if qErr := db.QueryRow(ctx, `
			SELECT indexdef FROM pg_indexes
			WHERE schemaname = $1 AND tablename = 'events' AND indexname = 'idx_events_trace'
		`, schema).Scan(&idxDef); qErr != nil {
			t.Fatalf("query idx_events_trace (%s): %v", strategy, qErr)
		}
		if !strings.Contains(idxDef, "trace_id") || !strings.Contains(idxDef, "IS NOT NULL") {
			t.Fatalf("unexpected trace index definition (%s): %s", strategy, idxDef)
		}

		var corrCols int
		if cErr := db.QueryRow(ctx, `
			SELECT count(*) FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = 'events' AND column_name = 'correlation_id'
		`, schema).Scan(&corrCols); cErr != nil || corrCols != 0 {
			t.Fatalf("correlation_id column check (%s): count=%d err=%v", strategy, corrCols, cErr)
		}
	}

	checkLiveStrategy("schema_none", migrations.PartitionStrategyNone)
	checkLiveStrategy("schema_native", migrations.PartitionStrategyNative)

	partmanSQL, err := migrations.SQL(&migrations.Config{
		EventsTable:      "events",
		StreamHeadsTable: "stream_heads",
		Partitioning: migrations.PartitionConfig{
			Strategy:      migrations.PartitionStrategyPartman,
			PartitionSize: 1000,
			PartmanSchema: "partman",
		},
	})
	if err != nil {
		t.Fatalf("migrations.SQL partman: %v", err)
	}
	if !strings.Contains(partmanSQL, "idx_events_trace") || strings.Contains(partmanSQL, "correlation_id") {
		t.Fatalf("partman DDL trace/correlation mismatch")
	}
}
