package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
)

// GetCheckpoint returns the last processed event cursor for a projection.
// It returns a zero store.Cursor if no checkpoint exists.
func GetCheckpoint(ctx context.Context, db DB, table, projectionName string) (store.Cursor, error) {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_transaction_id, last_position
		FROM %s
		WHERE projection_name = $1
	`, table)

	var cursor store.Cursor
	err := db.QueryRow(ctx, query, projectionName).Scan(&cursor.TransactionID, &cursor.GlobalPosition)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.Cursor{}, nil
		}
		return store.Cursor{}, fmt.Errorf("get checkpoint for projection %s: %w", projectionName, err)
	}

	return cursor, nil
}

// GetCheckpointForUpdate returns the last processed event cursor for a
// projection while locking the checkpoint row for update.
func GetCheckpointForUpdate(ctx context.Context, tx pgx.Tx, table, projectionName string) (store.Cursor, error) {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		SELECT last_transaction_id, last_position
		FROM %s
		WHERE projection_name = $1
		FOR UPDATE
	`, table)

	var cursor store.Cursor
	err := tx.QueryRow(ctx, query, projectionName).Scan(&cursor.TransactionID, &cursor.GlobalPosition)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return store.Cursor{}, nil
		}
		return store.Cursor{}, fmt.Errorf("lock checkpoint for projection %s: %w", projectionName, err)
	}

	return cursor, nil
}

// SaveCheckpoint upserts the checkpoint cursor monotonically for a projection within the given transaction.
func SaveCheckpoint(ctx context.Context, tx pgx.Tx, table, projectionName string, cursor store.Cursor) error {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %[1]s (projection_name, last_transaction_id, last_position, created_at, updated_at)
		VALUES ($1, $2::xid8, $3::bigint, NOW(), NOW())
		ON CONFLICT (projection_name)
		DO UPDATE SET
			last_transaction_id = CASE
				WHEN (EXCLUDED.last_transaction_id, EXCLUDED.last_position) > (%[1]s.last_transaction_id, %[1]s.last_position)
				THEN EXCLUDED.last_transaction_id
				ELSE %[1]s.last_transaction_id
			END,
			last_position = CASE
				WHEN (EXCLUDED.last_transaction_id, EXCLUDED.last_position) > (%[1]s.last_transaction_id, %[1]s.last_position)
				THEN EXCLUDED.last_position
				ELSE %[1]s.last_position
			END,
			updated_at = NOW()
	`, table)

	if _, err := tx.Exec(
		ctx,
		query,
		projectionName,
		strconv.FormatUint(cursor.TransactionID, 10),
		cursor.GlobalPosition,
	); err != nil {
		return fmt.Errorf("save checkpoint for projection %s: %w", projectionName, err)
	}

	return nil
}

// EnsureCheckpointExists creates a checkpoint row if it does not exist (zero cursor).
func EnsureCheckpointExists(ctx context.Context, db DB, table, projectionName string) error {
	table = resolveTableName(table, DefaultProjectionCheckpointsTable)

	//nolint:gosec // G201: table name comes from trusted configuration.
	query := fmt.Sprintf(`
		INSERT INTO %s (projection_name, last_transaction_id, last_position, created_at, updated_at)
		VALUES ($1, '0'::xid8, 0, NOW(), NOW())
		ON CONFLICT (projection_name) DO NOTHING
	`, table)

	if _, err := db.Exec(ctx, query, projectionName); err != nil {
		return fmt.Errorf("ensure checkpoint for projection %s: %w", projectionName, err)
	}

	return nil
}
