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

func TestAdoptRequireAndTxFromContext(t *testing.T) {
	t.Parallel()

	if tx, ok := postgres.TxFromContext(nil); tx != nil || ok { //nolint:staticcheck // SA1012: intentionally test nil ctx guard
		t.Fatalf("TxFromContext(nil) = (%v, %v), want (nil, false)", tx, ok)
	}
	if tx, ok := postgres.TxFromContext(context.Background()); tx != nil || ok {
		t.Fatalf("TxFromContext(background) = (%v, %v), want (nil, false)", tx, ok)
	}
	if _, err := postgres.RequireTx(nil); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("RequireTx(nil) err = %v, want ErrNilContext", err)
	}
	if _, err := postgres.RequireTx(context.Background()); !errors.Is(err, postgres.ErrMissingTx) {
		t.Fatalf("RequireTx(background) err = %v, want ErrMissingTx", err)
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
	reqTx, err := postgres.RequireTx(ctxWithTx)
	if err != nil || reqTx != tx1 {
		t.Fatalf("RequireTx = (%v, %v), want (%v, nil)", reqTx, err, tx1)
	}

	if _, err := postgres.AdoptTx(ctxWithTx, tx2); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested AdoptTx err = %v, want ErrTxAlreadyActive", err)
	}
}

func TestInTxAndInReadTx_StrictNonReentrancyAndOptions(t *testing.T) {
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

	// InReadTx forwards IsoLevel/DeferrableMode, forces pgx.ReadOnly even if ReadWrite was supplied,
	// and ignores nil TxOptions cleanly.
	err := postgres.InReadTx(context.Background(), db, func(ctx context.Context, tx pgx.Tx) error {
		ctxTx, ok := postgres.TxFromContext(ctx)
		if !ok || ctxTx != tx {
			t.Fatal("expected adopted tx inside InReadTx callback")
		}
		return nil
	}, nil, postgres.WithTxOptions(pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}))
	if err != nil {
		t.Fatalf("InReadTx: %v", err)
	}
	tx0 := db.txs[0]
	if tx0.txOpts.AccessMode != pgx.ReadOnly ||
		tx0.txOpts.IsoLevel != pgx.RepeatableRead ||
		tx0.txOpts.DeferrableMode != pgx.Deferrable ||
		!tx0.committed {
		t.Fatalf("unexpected InReadTx opts/state: %+v", tx0)
	}

	// All starters forbid reentrancy strictly (zero tx inheritance!).
	outer := &spyTx{db: db, id: 99}
	outerCtx, err := postgres.AdoptTx(context.Background(), outer)
	if err != nil {
		t.Fatalf("AdoptTx: %v", err)
	}
	if err := postgres.InReadTx(outerCtx, db, func(context.Context, pgx.Tx) error {
		t.Fatal("InReadTx callback must not run when outer tx active")
		return nil
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InReadTx over AdoptTx err = %v, want ErrTxAlreadyActive", err)
	}
	if err := postgres.InTx(outerCtx, db, func(context.Context, pgx.Tx) error {
		t.Fatal("InTx callback must not run when outer tx active")
		return nil
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InTx over AdoptTx err = %v, want ErrTxAlreadyActive", err)
	}
	if err := postgres.InTx(context.Background(), db, func(txCtx context.Context, _ pgx.Tx) error {
		return postgres.InTx(txCtx, db, func(context.Context, pgx.Tx) error { return nil })
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InTx inside InTx err = %v, want ErrTxAlreadyActive", err)
	}
	if err := postgres.InTx(context.Background(), db, func(txCtx context.Context, _ pgx.Tx) error {
		return postgres.InReadTx(txCtx, db, func(context.Context, pgx.Tx) error { return nil })
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InReadTx inside InTx err = %v, want ErrTxAlreadyActive", err)
	}
	if err := postgres.InReadTx(context.Background(), db, func(txCtx context.Context, _ pgx.Tx) error {
		return postgres.InTx(txCtx, db, func(context.Context, pgx.Tx) error { return nil })
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InTx inside InReadTx err = %v, want ErrTxAlreadyActive", err)
	}
	if err := postgres.InReadTx(context.Background(), db, func(txCtx context.Context, _ pgx.Tx) error {
		return postgres.InReadTx(txCtx, db, func(context.Context, pgx.Tx) error { return nil })
	}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("InReadTx inside InReadTx err = %v, want ErrTxAlreadyActive", err)
	}

	// Panic recovery rolls back both InTx and InReadTx cleanly and re-panics original value.
	assertPanicsWith(t, "in-tx-panic", func() {
		err := postgres.InTx(context.Background(), db, func(context.Context, pgx.Tx) error {
			panic("in-tx-panic")
		})
		t.Fatalf("expected panic, got %v", err)
	})
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected InTx panic rollback")
	}
	assertPanicsWith(t, "in-read-tx-panic", func() {
		err := postgres.InReadTx(context.Background(), db, func(context.Context, pgx.Tx) error {
			panic("in-read-tx-panic")
		})
		t.Fatalf("expected panic, got %v", err)
	})
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected InReadTx panic rollback")
	}
}

func TestTxLifecycle_RollbackDeadlinesErrorsAndBeginGuards(t *testing.T) {
	t.Parallel()

	// Custom positive RollbackTimeout vs zero/negative falling back to 5s.
	db := &spyDB{}
	for _, tc := range []struct {
		name        string
		timeout     time.Duration
		wantMinWait time.Duration
	}{
		{name: "custom 2s", timeout: 2 * time.Second, wantMinWait: time.Second},
		{name: "zero falls back to 5s", timeout: 0, wantMinWait: 3 * time.Second},
		{name: "negative falls back to 5s", timeout: -time.Second, wantMinWait: 3 * time.Second},
	} {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		primaryErr := command.Reject(errors.New("domain rejection"))
		start := time.Now()
		err := postgres.InTx(canceledCtx, db, func(context.Context, pgx.Tx) error {
			return primaryErr
		}, postgres.WithRollbackTimeout(tc.timeout), postgres.WithTxOptions(pgx.TxOptions{IsoLevel: pgx.Serializable}))
		if !errors.Is(err, primaryErr) || !command.IsRejected(err) {
			t.Fatalf("%s: expected primary rejection error, got %v", tc.name, err)
		}
		lastTx := db.txs[db.txCount()-1]
		if lastTx.txOpts.IsoLevel != pgx.Serializable {
			t.Fatalf("%s: IsoLevel = %q, want serializable", tc.name, lastTx.txOpts.IsoLevel)
		}
		if lastTx.rollbackCtxErr != nil {
			t.Fatalf("%s: rollback ctx unexpectedly canceled: %v", tc.name, lastTx.rollbackCtxErr)
		}
		if !lastTx.hasRollbackDeadline || lastTx.rollbackDeadline.Sub(start) < tc.wantMinWait {
			t.Fatalf("%s: rollback deadline=%v has=%v start=%v, want >= %v", tc.name, lastTx.rollbackDeadline, lastTx.hasRollbackDeadline, start, tc.wantMinWait)
		}
	}

	// Commit failure rolls back tx; ErrTxClosed on rollback is suppressed; non-closed rollback err is joined.
	commitFail := errors.New("commit failed")
	db.nextCommit = commitFail
	if err := postgres.InTx(context.Background(), db, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, commitFail) {
		t.Fatalf("Commit err = %v, want %v", err, commitFail)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected Commit failure to trigger Rollback")
	}

	db.nextCommit = commitFail
	db.nextRollback = pgx.ErrTxClosed
	errClosedSuppressed := postgres.InTx(context.Background(), db, func(context.Context, pgx.Tx) error { return nil })
	if !errors.Is(errClosedSuppressed, commitFail) || errors.Is(errClosedSuppressed, pgx.ErrTxClosed) {
		t.Fatalf("ErrTxClosed should be suppressed on rollback, got: %v", errClosedSuppressed)
	}

	rbFail := errors.New("rollback broken")
	db.nextCommit = commitFail
	db.nextRollback = rbFail
	errJoined := postgres.InTx(context.Background(), db, func(context.Context, pgx.Tx) error { return nil })
	if !errors.Is(errJoined, commitFail) || !errors.Is(errJoined, rbFail) {
		t.Fatalf("expected joined Commit+Rollback errors, got: %v", errJoined)
	}

	// BeginTx returning error or (nil, nil) or typed-nil returns clean error without panicking.
	beginFail := errors.New("begin failed")
	if err := postgres.InTx(context.Background(), &spyDB{beginErr: beginFail}, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, beginFail) {
		t.Fatalf("BeginTx err = %v, want %v", err, beginFail)
	}
	if err := postgres.InTx(context.Background(), &spyDB{returnNilTx: true}, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, postgres.ErrNilTx) {
		t.Fatalf("BeginTx(nil, nil) err = %v, want ErrNilTx", err)
	}
	if err := postgres.InTx(context.Background(), &spyDB{typedNilTx: true}, func(context.Context, pgx.Tx) error { return nil }); !errors.Is(err, postgres.ErrNilTx) {
		t.Fatalf("BeginTx(typed-nil, nil) err = %v, want ErrNilTx", err)
	}
}

func TestTxAndQueryMiddleware_NonReentrantAndGuards(t *testing.T) {
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
	if _, err := postgres.NewQueryTxMiddleware(typedNilDB); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("NewQueryTxMiddleware(typed-nil) err = %v, want ErrNilDB", err)
	}

	assertPanics(t, "MustTxMiddleware", func() {
		postgres.MustTxMiddleware(nil)
	})
	assertPanics(t, "MustQueryTxMiddleware", func() {
		postgres.MustQueryTxMiddleware(nil)
	})

	db := &spyDB{}
	txMW := postgres.MustTxMiddleware(db, postgres.WithTxOptions(pgx.TxOptions{IsoLevel: pgx.Serializable}))
	qMW := postgres.MustQueryTxMiddleware(db, postgres.WithTxOptions(pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	}))

	if err := txMW(func(context.Context, any) error { return nil })(nil, "cmd"); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("txMW(nil ctx) err = %v, want ErrNilContext", err)
	}
	if _, err := qMW(func(context.Context, any) (any, error) { return nil, nil })(nil, "q"); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("qMW(nil ctx) err = %v, want ErrNilContext", err)
	}

	// Nesting TxMiddleware inside TxMiddleware or QueryTxMiddleware inside TxMiddleware fails with ErrTxAlreadyActive.
	nestedErr := txMW(func(ctx context.Context, cmd any) error {
		return txMW(func(context.Context, any) error { return nil })(ctx, cmd)
	})(context.Background(), "cmd")
	if !errors.Is(nestedErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested TxMiddleware err = %v, want ErrTxAlreadyActive", nestedErr)
	}
	if db.txCount() != 1 || !db.txs[0].rolledBack || db.txs[0].txOpts.IsoLevel != pgx.Serializable {
		t.Fatalf("expected outer serializable tx rolled back, state=%+v", db.txs[0])
	}

	nestedQErr := txMW(func(ctx context.Context, _ any) error {
		_, qErr := qMW(func(context.Context, any) (any, error) { return nil, nil })(ctx, "q")
		return qErr
	})(context.Background(), "cmd")
	if !errors.Is(nestedQErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("QueryTxMiddleware inside TxMiddleware err = %v, want ErrTxAlreadyActive", nestedQErr)
	}

	// BeginTx failure inside middlewares surfaces directly.
	beginFail := errors.New("middleware begin boom")
	if err := postgres.MustTxMiddleware(&spyDB{beginErr: beginFail})(func(context.Context, any) error { return nil })(context.Background(), "c"); !errors.Is(err, beginFail) {
		t.Fatalf("txMW BeginTx err = %v, want %v", err, beginFail)
	}
	if _, err := postgres.MustQueryTxMiddleware(&spyDB{beginErr: beginFail})(func(context.Context, any) (any, error) { return nil, nil })(context.Background(), "q"); !errors.Is(err, beginFail) {
		t.Fatalf("qMW BeginTx err = %v, want %v", err, beginFail)
	}

	// Panic inside TxMiddleware & QueryTxMiddleware rolls back tx cleanly and re-panics.
	assertPanicsWith(t, "boom", func() {
		err := txMW(func(context.Context, any) error {
			panic("boom")
		})(context.Background(), "cmd")
		t.Fatalf("expected panic, got err=%v", err)
	})
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected txMW panic rolled back")
	}

	assertPanicsWith(t, "q-boom", func() {
		_, err := qMW(func(context.Context, any) (any, error) {
			panic("q-boom")
		})(context.Background(), "q")
		t.Fatalf("expected panic, got err=%v", err)
	})
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected qMW panic rolled back")
	}

	// QueryTxMiddleware forces ReadOnly while forwarding IsoLevel and returns result.
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
	lastQTx := db.txs[db.txCount()-1]
	if lastQTx.txOpts.AccessMode != pgx.ReadOnly || lastQTx.txOpts.IsoLevel != pgx.RepeatableRead || !lastQTx.committed {
		t.Fatalf("unexpected query tx state/opts: %+v", lastQTx)
	}

	// QueryTxMiddleware discards non-nil result when handler returns error and rolls back tx.
	qErr := errors.New("query failed")
	res, err = qMW(func(context.Context, any) (any, error) { return "stale-view", qErr })(context.Background(), "q")
	if res != nil || !errors.Is(err, qErr) {
		t.Fatalf("qMW error result = (%v, %v), want (nil, %v)", res, err, qErr)
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
