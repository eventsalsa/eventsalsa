//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

type creditCmd struct {
	id     string
	amount int
}

func TestNarrowPipeline_CausalChainInlineProjectionsAndQueryReadOnly(t *testing.T) {
	pool := setupPool(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("eventMapAccountCodec: %v", err)
	}

	var batchSaw int
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         pool,
		EventStore: es,
		StreamType: "Account",
		Codec:      codec,
		BatchProjections: []postgres.BatchProjection{
			postgres.BatchProjectionFunc(func(_ context.Context, _ pgx.Tx, events []store.PersistedEvent) error {
				batchSaw += len(events)
				return nil
			}),
		},
		Projections: []postgres.Projection{
			accountBalanceProjection(codec),
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	var (
		acquiredDuringDecide []int32
		seenCausationIDs     []string
		seenCommandIDs       []string
	)
	h := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, accountDecider()),
		func(c creditCmd) string { return c.id },
		func(_ accountState, c creditCmd) ([]any, error) {
			// Prove no DB connection or tx is held during domain decision logic.
			acquiredDuringDecide = append(acquiredDuringDecide, pool.Stat().AcquiredConns())
			if c.amount == 0 {
				return nil, nil
			}
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	)

	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf:     postgres.IsConcurrencyConflict,
			}),
		),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	wrappedCmdHandler := command.Func[creditCmd](func(cCtx context.Context, c creditCmd) error {
		seenCausationIDs = append(seenCausationIDs, cqrs.CausationIDFromContext(cCtx))
		seenCommandIDs = append(seenCommandIDs, cqrs.CommandIDFromContext(cCtx))
		return h.Handle(cCtx, c)
	})
	command.MustRegister(bus, wrappedCmdHandler)

	streamID := "acc-" + uuid.NewString()
	traceID := "trace-" + uuid.NewString()
	reqCtx := cqrs.WithTraceID(ctx, traceID)

	if dispatchErr := bus.Dispatch(reqCtx, creditCmd{id: streamID, amount: 100}); dispatchErr != nil {
		t.Fatalf("Dispatch #1: %v", dispatchErr)
	}
	// Zero-event decision leaves stream_heads untouched at version 1.
	if dispatchErr := bus.Dispatch(reqCtx, creditCmd{id: streamID, amount: 0}); dispatchErr != nil {
		t.Fatalf("Dispatch noop: %v", dispatchErr)
	}

	for i, n := range acquiredDuringDecide {
		if n != 0 {
			t.Fatalf("decide invocation %d held %d pooled connections, want 0", i, n)
		}
	}

	// Read persisted Event #1 directly from Postgres and bridge causation into Command #2.
	var (
		ev1ID    uuid.UUID
		ev1Trace string
		ev1Cause string
		ev1TxID  uint64
	)
	if scanErr := pool.QueryRow(ctx, `
		SELECT event_id, trace_id, causation_id, transaction_id::text::bigint
		FROM events
		WHERE stream_type = 'Account' AND stream_id = $1 AND stream_version = 1
	`, streamID).Scan(&ev1ID, &ev1Trace, &ev1Cause, &ev1TxID); scanErr != nil {
		t.Fatalf("query event 1: %v", scanErr)
	}
	if ev1Trace != traceID || ev1Cause != seenCommandIDs[0] || ev1TxID == 0 {
		t.Fatalf("unexpected event 1 causality: id=%s trace=%q cause=%q (want cmd=%q) tx=%d", ev1ID, ev1Trace, ev1Cause, seenCommandIDs[0], ev1TxID)
	}

	hopCtx := postgres.ContextWithEventCausation(ctx, store.PersistedEvent{
		EventID: ev1ID,
		TraceID: store.NullString{String: ev1Trace, Valid: true},
	})
	if dispatchErr := bus.Dispatch(hopCtx, creditCmd{id: streamID, amount: 50}); dispatchErr != nil {
		t.Fatalf("Dispatch #2: %v", dispatchErr)
	}

	// Command #2 received Event #1 as its CausationID, and emitted Event #2 caused by Command #2.
	if len(seenCausationIDs) != 3 || seenCausationIDs[2] != ev1ID.String() {
		t.Fatalf("command #2 CausationID = %q, want Event #1 id %q", seenCausationIDs[2], ev1ID.String())
	}
	var (
		ev2Trace string
		ev2Cause string
	)
	if scanErr := pool.QueryRow(ctx, `
		SELECT trace_id, causation_id
		FROM events
		WHERE stream_type = 'Account' AND stream_id = $1 AND stream_version = 2
	`, streamID).Scan(&ev2Trace, &ev2Cause); scanErr != nil {
		t.Fatalf("query event 2: %v", scanErr)
	}
	if ev2Trace != traceID || ev2Cause != seenCommandIDs[2] {
		t.Fatalf("event 2 causality = (%q, %q), want (%q, %q)", ev2Trace, ev2Cause, traceID, seenCommandIDs[2])
	}
	if batchSaw != 2 {
		t.Fatalf("batchSaw = %d, want 2", batchSaw)
	}

	// Query read-only middleware serves projected view and blocks accidental writes (SQLSTATE 25006).
	qBus, err := query.NewBus(query.WithMiddleware(postgres.MustQueryTxMiddleware(pool)))
	if err != nil {
		t.Fatalf("query.NewBus: %v", err)
	}
	query.MustRegister(qBus, query.Func[string, int](func(qCtx context.Context, targetID string) (int, error) {
		tx, ok := postgres.TxFromContext(qCtx)
		if !ok {
			return 0, errors.New("missing read tx")
		}
		var bal int
		if qErr := tx.QueryRow(qCtx, `SELECT balance FROM order_overview_v1 WHERE order_id = $1`, targetID).Scan(&bal); qErr != nil {
			return 0, qErr
		}
		return bal, nil
	}))
	query.MustRegister(qBus, query.Func[int, string](func(qCtx context.Context, _ int) (string, error) {
		tx, _ := postgres.TxFromContext(qCtx)
		_, execErr := tx.Exec(qCtx, `INSERT INTO audit_log (message) VALUES ('illegal read tx write')`)
		return "", execErr
	}))

	bal, err := query.Execute[int](ctx, qBus, streamID)
	if err != nil || bal != 150 {
		t.Fatalf("query balance = (%d, %v), want (150, nil)", bal, err)
	}

	_, writeInQueryErr := query.Execute[string](ctx, qBus, 1)
	var pgErr *pgconn.PgError
	if !errors.As(writeInQueryErr, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("expected SQLSTATE 25006 read_only_sql_transaction, got: %v", writeInQueryErr)
	}
}

type pgStateRepo struct {
	db postgres.TxBeginner
}

func (r *pgStateRepo) Load(ctx context.Context, id string) (accountState, int64, error) {
	var (
		st    accountState
		ver   int64
		found bool
	)
	err := postgres.InReadTx(ctx, r.db, func(txCtx context.Context, tx pgx.Tx) error {
		st.ID = id
		qErr := tx.QueryRow(txCtx, `
			SELECT balance, version FROM state_accounts WHERE account_id = $1
		`, id).Scan(&st.Balance, &ver)
		if errors.Is(qErr, pgx.ErrNoRows) {
			return nil
		}
		if qErr != nil {
			return qErr
		}
		found = true
		return nil
	})
	if err != nil {
		return accountState{}, 0, err
	}
	if !found {
		return accountState{}, 0, persistence.ErrNotFound
	}
	return st, ver, nil
}

func (r *pgStateRepo) Save(ctx context.Context, id string, expectedVersion int64, state accountState) error {
	return postgres.InTx(ctx, r.db, func(txCtx context.Context, tx pgx.Tx) error {
		if expectedVersion == 0 {
			_, insErr := tx.Exec(txCtx, `
				INSERT INTO state_accounts (account_id, balance, version)
				VALUES ($1, $2, 1)
			`, id, state.Balance)
			var pgErr *pgconn.PgError
			if errors.As(insErr, &pgErr) && pgErr.Code == "23505" {
				return persistence.ErrConcurrencyConflict
			}
			return insErr
		}
		tag, updErr := tx.Exec(txCtx, `
			UPDATE state_accounts SET balance = $2, version = version + 1
			WHERE account_id = $1 AND version = $3
		`, id, state.Balance, expectedVersion)
		if updErr != nil {
			return updErr
		}
		if tag.RowsAffected() == 0 {
			return persistence.ErrConcurrencyConflict
		}
		return nil
	})
}

func TestStateRepository_PostgresNarrowInReadTxAndInTx(t *testing.T) {
	pool := setupPool(t)
	ctx := context.Background()

	var connsInDecide int32 = -1
	h := persistence.MustNewCommandHandler(
		persistence.FromStateRepository(&pgStateRepo{db: pool}, accountDecider()),
		func(c creditCmd) string { return c.id },
		func(_ accountState, c creditCmd) ([]any, error) {
			connsInDecide = pool.Stat().AcquiredConns()
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	)

	id := "state-" + uuid.NewString()
	if err := h.Handle(ctx, creditCmd{id: id, amount: 75}); err != nil {
		t.Fatalf("Handle create: %v", err)
	}
	if connsInDecide != 0 {
		t.Fatalf("AcquiredConns during decide = %d, want 0", connsInDecide)
	}
	if err := h.Handle(ctx, creditCmd{id: id, amount: 25}); err != nil {
		t.Fatalf("Handle update: %v", err)
	}

	var (
		bal int
		ver int64
	)
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM state_accounts WHERE account_id = $1`, id).Scan(&bal, &ver); err != nil {
		t.Fatalf("query state_accounts: %v", err)
	}
	if bal != 100 || ver != 2 {
		t.Fatalf("state_accounts = (bal=%d, ver=%d), want (100, 2)", bal, ver)
	}
}
