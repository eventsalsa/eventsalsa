package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/query"
)

const defaultRollbackTimeout = 5 * time.Second

type txContextKey struct{}

// TxBeginner starts PostgreSQL transactions (satisfied by *pgxpool.Pool and *pgx.Conn).
type TxBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type txConfig struct {
	txOptions       pgx.TxOptions
	rollbackTimeout time.Duration
}

// TxOption configures PostgreSQL transaction acquisition and rollback behavior.
type TxOption func(*txConfig)

// WithTxOptions sets the pgx.TxOptions used when opening a transaction.
// Note that query middleware and InReadTx always enforce pgx.ReadOnly access mode.
//
//nolint:gocritic // hugeParam: matches pgx.TxOptions value API
func WithTxOptions(opts pgx.TxOptions) TxOption {
	return func(c *txConfig) {
		c.txOptions = opts
	}
}

// WithRollbackTimeout sets the deadline used when rolling back failed or panicked
// transactions over a detached context. Non-positive values are ignored.
func WithRollbackTimeout(d time.Duration) TxOption {
	return func(c *txConfig) {
		if d > 0 {
			c.rollbackTimeout = d
		}
	}
}

func buildTxConfig(readOnly bool, opts []TxOption) txConfig {
	cfg := txConfig{
		rollbackTimeout: defaultRollbackTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if readOnly {
		cfg.txOptions.AccessMode = pgx.ReadOnly
	}
	return cfg
}

// AdoptTx attaches tx onto a derived context. It is strictly non-reentrant and returns
// ErrTxAlreadyActive if ctx already carries an active transaction.
func AdoptTx(ctx context.Context, tx pgx.Tx) (context.Context, error) {
	if ctx == nil {
		return nil, persistence.ErrNilContext
	}
	if isNilValue(tx) {
		return nil, ErrNilTx
	}
	if _, ok := TxFromContext(ctx); ok {
		return nil, ErrTxAlreadyActive
	}
	return context.WithValue(ctx, txContextKey{}, tx), nil
}

// TxFromContext returns the active transaction carried by ctx, if any.
func TxFromContext(ctx context.Context) (pgx.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(txContextKey{}).(pgx.Tx)
	if !ok || isNilValue(tx) {
		return nil, false
	}
	return tx, true
}

// RequireTx retrieves the active transaction carried by ctx or returns ErrMissingTx if none
// is active (returning persistence.ErrNilContext when ctx is nil).
func RequireTx(ctx context.Context) (pgx.Tx, error) {
	if ctx == nil {
		return nil, persistence.ErrNilContext
	}
	tx, ok := TxFromContext(ctx)
	if !ok {
		return nil, ErrMissingTx
	}
	return tx, nil
}

// InTx starts and manages a new read-write transaction on db for the duration of fn.
// It is strictly non-reentrant and returns ErrTxAlreadyActive if ctx already carries an
// active transaction.
func InTx(
	ctx context.Context,
	db TxBeginner,
	fn func(ctx context.Context, tx pgx.Tx) error,
	opts ...TxOption,
) error {
	return runScopedTx(ctx, db, false, fn, opts...)
}

// InReadTx starts and manages a short pgx.ReadOnly transaction on db for the duration of fn.
// It is strictly non-reentrant and returns ErrTxAlreadyActive if ctx already carries an
// active transaction.
func InReadTx(
	ctx context.Context,
	db TxBeginner,
	fn func(ctx context.Context, tx pgx.Tx) error,
	opts ...TxOption,
) error {
	return runScopedTx(ctx, db, true, fn, opts...)
}

func runScopedTx(
	ctx context.Context,
	db TxBeginner,
	readOnly bool,
	fn func(ctx context.Context, tx pgx.Tx) error,
	opts ...TxOption,
) error {
	if ctx == nil {
		return persistence.ErrNilContext
	}
	if fn == nil {
		return ErrNilFunc
	}
	if isNilValue(db) {
		return ErrNilDB
	}
	if _, ok := TxFromContext(ctx); ok {
		return ErrTxAlreadyActive
	}
	cfg := buildTxConfig(readOnly, opts)
	return beginAndRunTx(ctx, db, cfg.txOptions, cfg.rollbackTimeout, fn)
}

// NewTxMiddleware returns command middleware that opens a PostgreSQL transaction per
// command dispatch and injects it into context via AdoptTx. It is strictly non-reentrant
// and returns ErrTxAlreadyActive if nested inside an existing transaction.
func NewTxMiddleware(db TxBeginner, opts ...TxOption) (command.Middleware, error) {
	if isNilValue(db) {
		return nil, ErrNilDB
	}
	cfg := buildTxConfig(false, opts)
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) error {
			if ctx == nil {
				return persistence.ErrNilContext
			}
			if _, ok := TxFromContext(ctx); ok {
				return ErrTxAlreadyActive
			}
			return beginAndRunTx(ctx, db, cfg.txOptions, cfg.rollbackTimeout, func(txCtx context.Context, _ pgx.Tx) error {
				return next(txCtx, cmd)
			})
		}
	}, nil
}

// MustTxMiddleware calls NewTxMiddleware and panics if db is nil.
func MustTxMiddleware(db TxBeginner, opts ...TxOption) command.Middleware {
	mw, err := NewTxMiddleware(db, opts...)
	if err != nil {
		panic(err)
	}
	return mw
}

// NewQueryTxMiddleware returns query middleware that wraps query execution in a
// pgx.ReadOnly transaction via AdoptTx. It is strictly non-reentrant.
func NewQueryTxMiddleware(db TxBeginner, opts ...TxOption) (query.Middleware, error) {
	if isNilValue(db) {
		return nil, ErrNilDB
	}
	cfg := buildTxConfig(true, opts)
	return func(next query.UntypedFunc) query.UntypedFunc {
		return func(ctx context.Context, q any) (any, error) {
			if ctx == nil {
				return nil, persistence.ErrNilContext
			}
			if _, ok := TxFromContext(ctx); ok {
				return nil, ErrTxAlreadyActive
			}
			var result any
			err := beginAndRunTx(ctx, db, cfg.txOptions, cfg.rollbackTimeout, func(txCtx context.Context, _ pgx.Tx) error {
				res, nextErr := next(txCtx, q)
				if nextErr != nil {
					return nextErr
				}
				result = res
				return nil
			})
			if err != nil {
				return nil, err
			}
			return result, nil
		}
	}, nil
}

// MustQueryTxMiddleware calls NewQueryTxMiddleware and panics if db is nil.
func MustQueryTxMiddleware(db TxBeginner, opts ...TxOption) query.Middleware {
	mw, err := NewQueryTxMiddleware(db, opts...)
	if err != nil {
		panic(err)
	}
	return mw
}

func beginAndRunTx(
	ctx context.Context,
	db TxBeginner,
	txOpts pgx.TxOptions, //nolint:gocritic // hugeParam: matches pgx.TxBeginner value contract
	rollbackTimeout time.Duration,
	fn func(ctx context.Context, tx pgx.Tx) error,
) error {
	tx, err := db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	if isNilValue(tx) {
		return ErrNilTx
	}
	txCtx, adoptErr := AdoptTx(ctx, tx)
	if adoptErr != nil {
		return errors.Join(adoptErr, rollbackTx(ctx, tx, rollbackTimeout))
	}
	return executeWithTx(ctx, txCtx, tx, rollbackTimeout, fn)
}

func executeWithTx(
	parentCtx context.Context,
	txCtx context.Context,
	tx pgx.Tx,
	rollbackTimeout time.Duration,
	fn func(ctx context.Context, tx pgx.Tx) error,
) (retErr error) {
	committed := false
	defer func() {
		if r := recover(); r != nil {
			committed = true
			_ = rollbackTx(parentCtx, tx, rollbackTimeout) //nolint:errcheck,gosec // best-effort rollback before preserving panic value
			panic(r)
		}
		if !committed {
			if rbErr := rollbackTx(parentCtx, tx, rollbackTimeout); rbErr != nil {
				retErr = errors.Join(retErr, rbErr)
			}
		}
	}()

	if err := fn(txCtx, tx); err != nil {
		return err
	}
	if err := tx.Commit(parentCtx); err != nil {
		return err
	}
	committed = true
	return nil
}

func rollbackTx(ctx context.Context, tx pgx.Tx, timeout time.Duration) error {
	if isNilValue(tx) {
		return nil
	}
	rbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := tx.Rollback(rbCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return err
	}
	return nil
}
