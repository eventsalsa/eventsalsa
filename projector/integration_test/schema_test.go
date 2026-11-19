//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"

	projectorpkg "eventsalsa.dev/projector"
	projectormigrations "eventsalsa.dev/projector/migrations"
	projectorpostgres "eventsalsa.dev/projector/postgres"
)

const infraSchema = "infra"

func generateProjectorSQLForSchema(t testing.TB, outputDir, schema string) []byte {
	t.Helper()

	config := &projectormigrations.Config{
		OutputFolder:               outputDir,
		OutputFilename:             "projector_" + schema + ".sql",
		ProjectorInstancesTable:    schema + ".projector_instances",
		ProjectionAssignmentsTable: schema + ".projection_assignments",
		ProjectionCheckpointsTable: schema + ".projection_checkpoints",
		ProjectorLeaderLeasesTable: schema + ".projector_leader_leases",
	}
	if err := projectormigrations.GeneratePostgres(config); err != nil {
		t.Fatalf("generate %s projector migration: %v", schema, err)
	}

	sqlBytes, err := os.ReadFile(filepath.Join(outputDir, config.OutputFilename))
	if err != nil {
		t.Fatalf("read %s projector migration: %v", schema, err)
	}

	return sqlBytes
}

func TestSchemaQualifiedProjectorTables(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	if _, err := controlDB.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, infraSchema)); err != nil {
		t.Fatalf("drop %s schema: %v", infraSchema, err)
	}
	t.Cleanup(func() {
		_, _ = controlDB.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, infraSchema))
	})

	ddl := generateProjectorSQLForSchema(t, t.TempDir(), infraSchema)
	if _, err := controlDB.Exec(ctx, string(ddl)); err != nil {
		t.Fatalf("execute schema-qualified projector migration: %v", err)
	}

	var gapSkipTableCount int
	if err := controlDB.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = 'projection_gap_skips'
	`, infraSchema).Scan(&gapSkipTableCount); err != nil {
		t.Fatalf("check projection_gap_skips absence: %v", err)
	}
	if gapSkipTableCount != 0 {
		t.Fatalf("found %d projection_gap_skips tables in schema %s, want 0", gapSkipTableCount, infraSchema)
	}

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("schema-proj", "projector-1", nil)

	options := append(defaultProjectorOptions(),
		projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease),
		projectorpkg.WithProjectorInstancesTable(infraSchema+".projector_instances"),
		projectorpkg.WithProjectionAssignmentsTable(infraSchema+".projection_assignments"),
		projectorpkg.WithProjectionCheckpointsTable(infraSchema+".projection_checkpoints"),
		projectorpkg.WithProjectorLeaderLeasesTable(infraSchema+".projector_leader_leases"),
	)
	harness := startTestProjectorWithTables(t, "projector-1",
		[]projectorpkg.Projection{projection},
		infraSchema+".projector_instances",
		options...,
	)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		checkpoint, err := projectorpostgres.GetCheckpoint(ctx, controlDB, infraSchema+".projection_checkpoints", projection.Name())
		if err != nil {
			return err
		}
		if checkpoint != latest {
			return fmt.Errorf("%s checkpoint=%+v want %+v", infraSchema, checkpoint, latest)
		}
		return nil
	})

	if checkpoint, err := projectorpostgres.GetCheckpoint(ctx, controlDB, projectorpostgres.DefaultProjectionCheckpointsTable, projection.Name()); err != nil || checkpoint != (store.Cursor{}) {
		t.Fatalf("default public checkpoint=%+v (err=%v), want zero cursor", checkpoint, err)
	}

	harness.stop(t)
}
