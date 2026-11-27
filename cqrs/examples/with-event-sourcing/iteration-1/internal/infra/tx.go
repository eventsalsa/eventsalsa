package infra

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
)

type txCtxKey struct{}

// ErrMissingTx indicates no active pgx.Tx was injected into context by WithPgxTx.
var ErrMissingTx = errors.New("missing pgx.Tx in context")

// WithPgxTx wraps command execution in a single PostgreSQL transaction.
func WithPgxTx(pool *pgxpool.Pool) command.Middleware {
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) (retErr error) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			committed := false
			defer func() {
				if !committed {
					if rbErr := tx.Rollback(ctx); rbErr != nil &&
						retErr == nil &&
						!errors.Is(rbErr, pgx.ErrTxClosed) {
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
		return nil, ErrMissingTx
	}
	return tx, nil
}
