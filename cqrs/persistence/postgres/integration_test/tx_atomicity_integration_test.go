//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	storepostgres "eventsalsa.dev/store/postgres"
)

type wideTransferCmd struct {
	streamA string
	streamB string
	amount  int
	fail    bool
	panicAt bool
}

func TestWideTxMiddleware_MultiStreamAtomicityAndPoolLeakStress(t *testing.T) {
	pool := setupPool(t, func(c *pgxpool.Config) {
		c.MaxConns = 2
	})
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	repoA, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: pool, EventStore: es, StreamType: "WalletA", Codec: codec,
	})
	if err != nil {
		t.Fatalf("repoA: %v", err)
	}
	repoB, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: pool, EventStore: es, StreamType: "WalletB", Codec: codec,
	})
	if err != nil {
		t.Fatalf("repoB: %v", err)
	}

	handlerErr := errors.New("intentional multi-stream abort")
	handler := command.Func[wideTransferCmd](func(cCtx context.Context, cmd wideTransferCmd) error {
		tx, ok := postgres.TxFromContext(cCtx)
		if !ok {
			return errors.New("expected active wide tx")
		}
		if err := repoA.Append(cCtx, cmd.streamA, 0, []any{accountCredited{Amount: cmd.amount}}); err != nil {
			return err
		}
		if _, err := tx.Exec(cCtx, `INSERT INTO audit_log (message) VALUES ($1)`, "transfer:"+cmd.streamA); err != nil {
			return err
		}
		if cmd.panicAt {
			panic("handler boom")
		}
		if cmd.fail {
			return handlerErr
		}
		return repoB.Append(cCtx, cmd.streamB, 0, []any{accountCredited{Amount: cmd.amount}})
	})

	nestedMW := postgres.MustTxMiddleware(pool)
	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(pool)))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, handler)
	command.MustRegister(bus, command.Func[string](func(cCtx context.Context, id string) error {
		return nestedMW(func(context.Context, any) error { return nil })(cCtx, id)
	}))

	// 1. Atomic success commits both streams + audit_log row under the exact same Postgres tx.
	sA := "wa-" + uuid.NewString()
	sB := "wb-" + uuid.NewString()
	if err := bus.Dispatch(ctx, wideTransferCmd{streamA: sA, streamB: sB, amount: 42}); err != nil {
		t.Fatalf("wide commit: %v", err)
	}

	var countAudit int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM audit_log WHERE message = $1`, "transfer:"+sA).Scan(&countAudit); err != nil {
		t.Fatalf("query audit: %v", err)
	}
	if countAudit != 1 {
		t.Fatalf("audit count = %d, want 1", countAudit)
	}

	// 2. Handler failure rolls back WalletA + audit_log completely.
	failA := "wa-fail-" + uuid.NewString()
	failB := "wb-fail-" + uuid.NewString()
	if err := bus.Dispatch(ctx, wideTransferCmd{streamA: failA, streamB: failB, amount: 10, fail: true}); !errors.Is(err, handlerErr) {
		t.Fatalf("wide fail err = %v, want %v", err, handlerErr)
	}
	if _, verA, loadErr := repoA.Load(ctx, failA); loadErr != nil || verA != 0 {
		t.Fatalf("failA ver = %d, err = %v; expected rolled back stream", verA, loadErr)
	}

	// 3. Handler panic rolls back WalletA + audit_log and propagates panic without leaking pool connection.
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
	if _, verP, loadErr := repoA.Load(ctx, panicA); loadErr != nil || verP != 0 {
		t.Fatalf("panicA ver = %d, err = %v; expected rolled back stream", verP, loadErr)
	}

	// 4. Nested TxMiddleware fails with ErrTxAlreadyActive.
	if err := bus.Dispatch(ctx, "nested-check"); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("nested middleware err = %v, want ErrTxAlreadyActive", err)
	}

	// 5. Tight MaxConns=2 stress: pre-canceled and deadline-aborted InTx cycles never leak connections.
	cancelBusHandler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repoA, accountDecider()),
		func(id string) string { return id },
		func(accountState, string) ([]any, error) { return []any{accountCredited{Amount: 1}}, nil },
	)
	for i := 0; i < 6; i++ {
		dlCtx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		time.Sleep(time.Millisecond)
		if err := cancelBusHandler.Handle(dlCtx, "leak-"+uuid.NewString()); err == nil {
			cancel()
			t.Fatal("expected deadline error on expired ctx")
		}
		cancel()
	}
	if err := cancelBusHandler.Handle(ctx, "healthy-"+uuid.NewString()); err != nil {
		t.Fatalf("pool wedged after canceled cycles: %v", err)
	}
}
