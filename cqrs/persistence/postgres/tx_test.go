package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
)

func TestAdoptAndTxFromContext(t *testing.T) {
	t.Parallel()

	if tx, ok := postgres.TxFromContext(nil); tx != nil || ok { //nolint:staticcheck // SA1012: intentionally test nil ctx guard
		t.Fatalf("TxFromContext(nil) = (%v, %v), want (nil, false)", tx, ok)
	}
	if tx, ok := postgres.TxFromContext(context.Background()); tx != nil || ok {
		t.Fatalf("TxFromContext(background) = (%v, %v), want (nil, false)", tx, ok)
	}

	var typedNilTx *spyTx
	tx1 := &spyTx{db: &spyDB{}, id: 1}
	tx2 := &spyTx{db: &spyDB{}, id: 2}

	if _, err := postgres.AdoptTx(nil, tx1); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("AdoptTx(nil ctx) err = %v, want ErrNilContext", err)
	}
	if _, err := postgres.AdoptTx(context.Background(), nil); !errors.Is(err, postgres.ErrNilTx) {
		t.Fatalf("AdoptTx(nil tx) err = %v, want ErrNilTx", err)
	}
	if _, err := postgres.AdoptTx(context.Background(), typedNilTx); !errors.Is(err, postgres.ErrNilTx) {
		t.Fatalf("AdoptTx(typed-nil tx) err = %v, want ErrNilTx", err)
	}

	ctxWithTx, err := postgres.AdoptTx(context.Background(), tx1)
	if err != nil {
		t.Fatalf("AdoptTx: %v", err)
	}
	gotTx, ok := postgres.TxFromContext(ctxWithTx)
	if !ok || gotTx != tx1 {
		t.Fatalf("TxFromContext = (%v, %v), want (%v, true)", gotTx, ok, tx1)
	}

	if _, err := postgres.AdoptTx(ctxWithTx, tx2); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested AdoptTx err = %v, want ErrTxAlreadyActive", err)
	}
}

func TestInTxAndInReadTx_LifecycleAndGuards(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	var typedNilDB *spyDB

	if err := postgres.InTx(nil, db, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("InTx(nil ctx) err = %v, want ErrNilContext", err)
	}
	if err := postgres.InTx(context.Background(), db, nil); !errors.Is(err, postgres.ErrNilFunc) {
		t.Fatalf("InTx(nil fn) err = %v, want ErrNilFunc", err)
	}
	if err := postgres.InTx(context.Background(), nil, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("InTx(nil db) err = %v, want ErrNilDB", err)
	}
	if err := postgres.InReadTx(context.Background(), typedNilDB, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("InReadTx(typed-nil db) err = %v, want ErrNilDB", err)
	}

	// Narrow InReadTx forces pgx.ReadOnly even with custom WithTxOptions.
	err := postgres.InReadTx(context.Background(), db, func(ctx context.Context, tx pgx.Tx) error {
		ctxTx, ok := postgres.TxFromContext(ctx)
		if !ok || ctxTx != tx {
			t.Fatal("expected adopted tx inside InReadTx callback")
		}
		return nil
	}, postgres.WithTxOptions(pgx.TxOptions{IsoLevel: pgx.RepeatableRead}), postgres.WithRollbackTimeout(0))
	if err != nil {
		t.Fatalf("InReadTx: %v", err)
	}
	if db.txCount() != 1 || db.txs[0].accessMode != pgx.ReadOnly || !db.txs[0].committed {
		t.Fatalf("unexpected tx state: count=%d tx=%+v", db.txCount(), db.txs[0])
	}

	// Active ctx tx is reused even when db == nil and is never committed/rolled back by inner InTx/InReadTx.
	outer := &spyTx{db: &spyDB{}, id: 99}
	outerCtx, err := postgres.AdoptTx(context.Background(), outer)
	if err != nil {
		t.Fatalf("AdoptTx: %v", err)
	}
	if err := postgres.InReadTx(outerCtx, nil, func(_ context.Context, tx pgx.Tx) error {
		if tx != outer {
			t.Fatalf("expected outer tx reuse, got %v", tx)
		}
		return nil
	}); err != nil {
		t.Fatalf("InReadTx reuse: %v", err)
	}
	if outer.committed || outer.rolledBack {
		t.Fatal("inner InReadTx must not commit or roll back active outer tx")
	}

	// Canceled caller ctx uses context.WithoutCancel during rollback and preserves primary/rollback errors.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	primaryErr := command.Reject(errors.New("domain rejection"))
	rbFail := errors.New("rollback broken")
	db.nextRollback = rbFail

	err = postgres.InTx(canceledCtx, db, func(context.Context, pgx.Tx) error {
		return primaryErr
	}, postgres.WithRollbackTimeout(2*time.Second))
	if !errors.Is(err, primaryErr) || !errors.Is(err, rbFail) || !command.IsRejected(err) {
		t.Fatalf("expected joined primary rejection + rollback error, got: %v", err)
	}
	lastTx := db.txs[db.txCount()-1]
	if lastTx.rollbackCtxErr != nil {
		t.Fatalf("rollback context should ignore parent cancellation, got: %v", lastTx.rollbackCtxErr)
	}

	// BeginTx failure & Commit failure surface cleanly.
	beginFail := errors.New("begin failed")
	dbFail := &spyDB{beginErr: beginFail}
	if err := postgres.InTx(context.Background(), dbFail, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, beginFail) {
		t.Fatalf("BeginTx err = %v, want %v", err, beginFail)
	}
	commitFail := errors.New("commit failed")
	db.nextCommit = commitFail
	if err := postgres.InTx(context.Background(), db, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, commitFail) {
		t.Fatalf("Commit err = %v, want %v", err, commitFail)
	}
}

func TestTxAndQueryMiddleware_NonReentrantAndPanicRollback(t *testing.T) {
	t.Parallel()

	var typedNilDB *spyDB
	if _, err := postgres.NewTxMiddleware(nil); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("NewTxMiddleware(nil) err = %v, want ErrNilDB", err)
	}
	if _, err := postgres.NewTxMiddleware(typedNilDB); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("NewTxMiddleware(typed-nil) err = %v, want ErrNilDB", err)
	}
	if _, err := postgres.NewQueryTxMiddleware(nil); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("NewQueryTxMiddleware(nil) err = %v, want ErrNilDB", err)
	}

	assertPanics(t, "MustTxMiddleware", func() {
		postgres.MustTxMiddleware(nil)
	})
	assertPanics(t, "MustQueryTxMiddleware", func() {
		postgres.MustQueryTxMiddleware(nil)
	})

	db := &spyDB{}
	txMW := postgres.MustTxMiddleware(db)
	qMW := postgres.MustQueryTxMiddleware(db)

	if err := txMW(func(context.Context, any) error { return nil })(nil, "cmd"); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("txMW(nil ctx) err = %v, want ErrNilContext", err)
	}
	if _, err := qMW(func(context.Context, any) (any, error) { return nil, nil })(nil, "q"); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("qMW(nil ctx) err = %v, want ErrNilContext", err)
	}

	// Nesting TxMiddleware inside TxMiddleware must fail with ErrTxAlreadyActive and roll back outer.
	nestedErr := txMW(func(ctx context.Context, cmd any) error {
		return txMW(func(context.Context, any) error { return nil })(ctx, cmd)
	})(context.Background(), "cmd")
	if !errors.Is(nestedErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested TxMiddleware err = %v, want ErrTxAlreadyActive", nestedErr)
	}
	if db.txCount() != 1 || !db.txs[0].rolledBack {
		t.Fatalf("expected outer tx rolled back, state=%+v", db.txs)
	}

	// Panic inside TxMiddleware rolls back tx cleanly and re-panics original value.
	assertPanicsWith(t, "boom", func() {
		err := txMW(func(context.Context, any) error {
			panic("boom")
		})(context.Background(), "cmd")
		t.Fatalf("expected panic, got err=%v", err)
	})
	if db.txCount() != 2 || !db.txs[1].rolledBack {
		t.Fatalf("expected panic tx rolled back, got tx=%+v", db.txs[1])
	}

	// QueryTxMiddleware executes inside pgx.ReadOnly tx and forwards result.
	res, err := qMW(func(ctx context.Context, q any) (any, error) {
		if _, ok := postgres.TxFromContext(ctx); !ok {
			t.Fatal("expected read tx in ctx")
		}
		s, ok := q.(string)
		if !ok {
			t.Fatalf("unexpected query type %T", q)
		}
		return s + "-view", nil
	})(context.Background(), "ord-1")
	if err != nil || res != "ord-1-view" {
		t.Fatalf("qMW = (%v, %v), want (ord-1-view, nil)", res, err)
	}
	if db.txCount() != 3 || db.txs[2].accessMode != pgx.ReadOnly || !db.txs[2].committed {
		t.Fatalf("unexpected query tx state: %+v", db.txs[2])
	}

	// QueryTxMiddleware rejects reentrant active tx and returns handler errors cleanly.
	outerCtx, adoptErr := postgres.AdoptTx(context.Background(), &spyTx{db: db, id: 77})
	if adoptErr != nil {
		t.Fatalf("AdoptTx: %v", adoptErr)
	}
	if _, err := qMW(func(context.Context, any) (any, error) { return nil, nil })(outerCtx, "q"); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("reentrant QueryTxMiddleware err = %v, want ErrTxAlreadyActive", err)
	}

	qErr := errors.New("query failed")
	if _, err := qMW(func(context.Context, any) (any, error) { return nil, qErr })(context.Background(), "q"); !errors.Is(err, qErr) {
		t.Fatalf("qMW err = %v, want %v", err, qErr)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected failed query tx rolled back")
	}
}

func assertPanics(t *testing.T, label string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s: expected panic", label)
		}
	}()
	fn()
}

func assertPanicsWith(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r != want {
			t.Fatalf("panic = %v, want %q", r, want)
		}
	}()
	fn()
}
