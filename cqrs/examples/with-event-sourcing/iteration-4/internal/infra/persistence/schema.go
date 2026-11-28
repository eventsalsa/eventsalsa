package persistence

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	snapshotmigrations "eventsalsa.dev/snapshot/migrations"
	"eventsalsa.dev/store/migrations"
)

// EnsureSchema installs eventsalsa/store tables, eventsalsa/snapshot tables, and order_overview_v1.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	cfg := migrations.DefaultConfig()
	ddl, err := migrations.SQL(&cfg)
	if err != nil {
		return fmt.Errorf("build store DDL: %w", err)
	}
	if _, execErr := pool.Exec(ctx, ddl); execErr != nil {
		return fmt.Errorf("apply store DDL: %w", execErr)
	}

	snapDDL, err := snapshotmigrations.FS.ReadFile("20260621084951_init_snapshots.sql")
	if err != nil {
		return fmt.Errorf("read snapshot DDL: %w", err)
	}
	if _, execErr := pool.Exec(ctx, string(snapDDL)); execErr != nil {
		return fmt.Errorf("apply snapshot DDL: %w", execErr)
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS order_overview_v1 (
			order_id    TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			status      TEXT NOT NULL,
			total_cents BIGINT NOT NULL,
			item_count  BIGINT NOT NULL DEFAULT 0,
			version     BIGINT NOT NULL
		);
		ALTER TABLE order_overview_v1
			ADD COLUMN IF NOT EXISTS item_count BIGINT NOT NULL DEFAULT 0;
	`)
	return err
}
