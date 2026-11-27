package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/store/migrations"
)

type txCtxKey struct{}

func withPgxTx(pool *pgxpool.Pool) command.Middleware {
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) (retErr error) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			committed := false
			defer func() {
				if !committed {
					if rbErr := tx.Rollback(ctx); rbErr != nil && retErr == nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
						retErr = rbErr
					}
				}
			}()

			if nextErr := next(context.WithValue(ctx, txCtxKey{}, tx), cmd); nextErr != nil {
				return nextErr
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return commitErr
			}
			committed = true
			return nil
		}
	}
}

func txFromContext(ctx context.Context) (pgx.Tx, error) {
	tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx)
	if !ok {
		return nil, errors.New("missing pgx.Tx in context")
	}
	return tx, nil
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
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
