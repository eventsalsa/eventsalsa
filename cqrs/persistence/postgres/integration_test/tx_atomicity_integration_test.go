//go:build integration

package integration_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	storepostgres "eventsalsa.dev/store/postgres"
)

type wideTransferCmd struct {
	streamA     string
	streamB     string
	readYourOwn *int
	amount      int
	fail        bool
	panicAt     bool
}

func TestWideTxMiddleware_MultiStreamAtomicityStrictNonReentrancyAndStress(t *testing.T) {
	pool := setupPool(t, func(c *pgxpool.Config) {
		c.MaxConns = 2
	})
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	// Narrow repository vs wide TxEventRepository cleanly enforce single tx ownership.
	narrowRepoA, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: pool, EventStore: es, StreamType: "WalletA", Codec: codec,
	})
	if err != nil {
		t.Fatalf("narrowRepoA: %v", err)
	}
	txRepoA, err := postgres.NewTxEventRepository(postgres.TxEventRepositoryConfig[any]{
		EventStore: es, StreamType: "WalletA", Codec: codec,
	})
	if err != nil {
		t.Fatalf("txRepoA: %v", err)
	}
	txRepoB, err := postgres.NewTxEventRepository(postgres.TxEventRepositoryConfig[any]{
		EventStore: es, StreamType: "WalletB", Codec: codec,
	})
	if err != nil {
		t.Fatalf("txRepoB: %v", err)
	}

	handlerErr := errors.New("intentional multi-stream abort")
	handler := command.Func[wideTransferCmd](func(cCtx context.Context, cmd wideTransferCmd) error {
		tx, reqErr := postgres.RequireTx(cCtx)
		if reqErr != nil {
			return reqErr
		}
		if appErr := txRepoA.Append(cCtx, cmd.streamA, 0, []any{accountCredited{Amount: cmd.amount}}); appErr != nil {
			return appErr
		}
		if cmd.readYourOwn != nil {
			evs, ver, loadErr := txRepoA.Load(cCtx, cmd.streamA)
			if loadErr != nil {
				return loadErr
			}
			if ver != 1 || len(evs) != 1 {
				return errors.New("expected read-your-writes inside active wide tx")
			}
			credited, ok := evs[0].(accountCredited)
			if !ok {
				return errors.New("unexpected event type")
			}
			*cmd.readYourOwn = credited.Amount
		}
		if _, execErr := tx.Exec(cCtx, `INSERT INTO audit_log (message) VALUES ($1)`, "transfer:"+cmd.streamA); execErr != nil {
			return execErr
		}
		if cmd.panicAt {
			panic("handler boom")
		}
		if cmd.fail {
			return handlerErr
		}
		// Demonstrate explicit AppendTx interop alongside TxEventRepository inside the same wide tx.
		return narrowRepoA.AppendTx(cCtx, tx, cmd.streamB, 0, []any{accountCredited{Amount: cmd.amount}})
	})

	nestedMW := postgres.MustTxMiddleware(pool)
	nestedQMW := postgres.MustQueryTxMiddleware(pool)

	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(pool)))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, handler)
	command.MustRegister(bus, command.Func[string](func(cCtx context.Context, id string) error {
		return nestedMW(func(context.Context, any) error { return nil })(cCtx, id)
	}))
	command.MustRegister(bus, command.Func[int](func(cCtx context.Context, _ int) error {
		_, qErr := nestedQMW(func(context.Context, any) (any, error) { return nil, nil })(cCtx, "q")
		return qErr
	}))

	// 0. Narrow EventRepository inside wide MustTxMiddleware fails with ErrTxAlreadyActive cleanly.
	badMixedBus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(pool)))
	if err != nil {
		t.Fatalf("badMixedBus: %v", err)
	}
	command.MustRegister(badMixedBus, persistence.MustNewCommandHandler(
		persistence.FromEventRepository(narrowRepoA, accountDecider()),
		func(s string) string { return s },
		func(accountState, string) ([]any, error) { return []any{accountCredited{Amount: 1}}, nil },
	))
	mixedID := "mixed-" + uuid.NewString()
	if mixErr := badMixedBus.Dispatch(ctx, mixedID); !errors.Is(mixErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("narrow repo inside TxMiddleware err = %v, want ErrTxAlreadyActive", mixErr)
	}
	if _, verMixed, loadErr := narrowRepoA.Load(ctx, mixedID); loadErr != nil || verMixed != 0 {
		t.Fatalf("expected 0 events after ErrTxAlreadyActive, got ver=%d err=%v", verMixed, loadErr)
	}

	// 1. Atomic success: TxEventRepository + AppendTx + SQL audit_log commit in the exact same txid.
	sA := "wa-" + uuid.NewString()
	sB := "wb-" + uuid.NewString()
	sawAmount := 0
	if dispatchErr := bus.Dispatch(ctx, wideTransferCmd{
		streamA:     sA,
		streamB:     sB,
		amount:      42,
		readYourOwn: &sawAmount,
	}); dispatchErr != nil {
		t.Fatalf("wide commit: %v", dispatchErr)
	}
	if sawAmount != 42 {
		t.Fatalf("read-your-writes amount = %d, want 42", sawAmount)
	}

	var txA, txB uint64
	if scanErr := pool.QueryRow(ctx, `SELECT transaction_id::text::bigint FROM events WHERE stream_id = $1`, sA).Scan(&txA); scanErr != nil {
		t.Fatalf("query txA: %v", scanErr)
	}
	if scanErr := pool.QueryRow(ctx, `SELECT transaction_id::text::bigint FROM events WHERE stream_id = $1`, sB).Scan(&txB); scanErr != nil {
		t.Fatalf("query txB: %v", scanErr)
	}
	if txA == 0 || txA != txB {
		t.Fatalf("wide multi-stream transaction_id = (%d, %d), want identical non-zero xid", txA, txB)
	}
	loadedA, verA, loadAErr := narrowRepoA.Load(ctx, sA)
	loadedB, verB, loadBErr := narrowRepoA.Load(ctx, sB)
	if loadAErr != nil || verA != 1 || !slices.Equal(loadedA, []any{accountCredited{Amount: 42}}) ||
		loadBErr != nil || verB != 1 || !slices.Equal(loadedB, []any{accountCredited{Amount: 42}}) {
		t.Fatalf("loaded streams = (%v, %d, %v) / (%v, %d, %v)", loadedA, verA, loadAErr, loadedB, verB, loadBErr)
	}

	var countAudit int
	if qErr := pool.QueryRow(ctx, `SELECT COUNT(*) FROM audit_log WHERE message = $1`, "transfer:"+sA).Scan(&countAudit); qErr != nil {
		t.Fatalf("query audit: %v", qErr)
	}
	if countAudit != 1 {
		t.Fatalf("audit count = %d, want 1", countAudit)
	}

	// 2. Handler failure rolls back WalletA, WalletB, and audit_log completely.
	failA := "wa-fail-" + uuid.NewString()
	failB := "wb-fail-" + uuid.NewString()
	if err := bus.Dispatch(ctx, wideTransferCmd{streamA: failA, streamB: failB, amount: 10, fail: true}); !errors.Is(err, handlerErr) {
		t.Fatalf("wide fail err = %v, want %v", err, handlerErr)
	}
	assertWideRolledBack(ctx, t, pool, narrowRepoA, txRepoB, failA, failB)

	// 3. Handler panic rolls back WalletA, WalletB, and audit_log and propagates panic without leaking conns.
	panicA := "wa-panic-" + uuid.NewString()
	panicB := "wb-panic-" + uuid.NewString()
	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		dispatchErr := bus.Dispatch(ctx, wideTransferCmd{streamA: panicA, streamB: panicB, amount: 10, panicAt: true})
		t.Fatalf("expected panic, got err=%v", dispatchErr)
	}()
	if !didPanic {
		t.Fatal("expected panic to propagate")
	}
	assertWideRolledBack(ctx, t, pool, narrowRepoA, txRepoB, panicA, panicB)

	// 4. Nested TxMiddleware and QueryTxMiddleware inside TxMiddleware fail with ErrTxAlreadyActive.
	if err := bus.Dispatch(ctx, "nested-check"); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested middleware err = %v, want ErrTxAlreadyActive", err)
	}
	if err := bus.Dispatch(ctx, 123); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested QueryTxMiddleware err = %v, want ErrTxAlreadyActive", err)
	}

	// 5. Serializable TxOption actually sets PostgreSQL transaction_isolation = 'serializable'.
	var isoLevel string
	if serErr := postgres.InTx(ctx, pool, func(txCtx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(txCtx, `SHOW transaction_isolation`).Scan(&isoLevel)
	}, postgres.WithTxOptions(pgx.TxOptions{IsoLevel: pgx.Serializable})); serErr != nil || isoLevel != "serializable" {
		t.Fatalf("WithTxOptions Serializable = (%q, %v), want ('serializable', nil)", isoLevel, serErr)
	}

	// 6. Mid-flight cancellation & expired deadlines on MaxConns=2 roll back via WithoutCancel cleanly.
	midStream := "mid-cancel-" + uuid.NewString()
	midCtx, midCancel := context.WithCancel(ctx)
	midErr := postgres.InTx(midCtx, pool, func(txCtx context.Context, tx pgx.Tx) error {
		if appErr := narrowRepoA.AppendTx(txCtx, tx, midStream, 0, []any{accountCredited{Amount: 99}}); appErr != nil {
			return appErr
		}
		midCancel()
		return context.Canceled
	})
	if !errors.Is(midErr, context.Canceled) {
		t.Fatalf("mid-flight cancel err = %v, want context.Canceled", midErr)
	}
	if _, verMid, loadErr := narrowRepoA.Load(ctx, midStream); loadErr != nil || verMid != 0 {
		t.Fatalf("expected mid-flight cancel rolled back, got ver=%d err=%v", verMid, loadErr)
	}

	cancelBusHandler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(narrowRepoA, accountDecider()),
		func(id string) string { return id },
		func(accountState, string) ([]any, error) { return []any{accountCredited{Amount: 1}}, nil },
	)
	for range 6 {
		dlCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		time.Sleep(time.Millisecond)
		if handleErr := cancelBusHandler.Handle(dlCtx, "leak-"+uuid.NewString()); handleErr == nil {
			cancel()
			t.Fatal("expected deadline error on expired ctx")
		}
		cancel()
	}
	if err := cancelBusHandler.Handle(ctx, "healthy-"+uuid.NewString()); err != nil {
		t.Fatalf("pool wedged after canceled cycles: %v", err)
	}
}

func assertWideRolledBack(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	narrowA *postgres.EventRepository[any],
	txRepoB *postgres.TxEventRepository[any],
	streamA string,
	streamB string,
) {
	t.Helper()

	if _, verA, err := narrowA.Load(ctx, streamA); err != nil || verA != 0 {
		t.Fatalf("%s ver = %d, err = %v; expected rolled back", streamA, verA, err)
	}
	if rbErr := postgres.InReadTx(ctx, pool, func(txCtx context.Context, _ pgx.Tx) error {
		_, verB, loadErr := txRepoB.Load(txCtx, streamB)
		if loadErr != nil {
			return loadErr
		}
		if verB != 0 {
			return errors.New("expected streamB rolled back")
		}
		return nil
	}); rbErr != nil {
		t.Fatalf("%s check failed: %v", streamB, rbErr)
	}
	var auditCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM audit_log WHERE message = $1`, "transfer:"+streamA).Scan(&auditCount); err != nil {
		t.Fatalf("query audit_log: %v", err)
	}
	if auditCount != 0 {
		t.Fatalf("audit_log count for %s = %d, want 0", streamA, auditCount)
	}
}
