package infra

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/store/migrations"
)

// EnsureSchema installs eventsalsa/store tables and the order_overview_v1 projection table.
func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	cfg := migrations.DefaultConfig()
	ddl, err := migrations.SQL(&cfg)
	if err != nil {
		return fmt.Errorf("build store DDL: %w", err)
	}
	if _, execErr := pool.Exec(ctx, ddl); execErr != nil {
		return fmt.Errorf("apply store DDL: %w", execErr)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS order_overview_v1 (
			order_id    TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			status      TEXT NOT NULL,
			total_cents BIGINT NOT NULL,
			version     BIGINT NOT NULL
		)
	`)
	return err
}
