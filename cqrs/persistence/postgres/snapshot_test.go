package postgres_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/snapshot"
	"eventsalsa.dev/store"
)

type depositCmd struct {
	id     string
	amount int
}

func depositID(c depositCmd) string { return c.id }

func decideDeposit(_ accountState, c depositCmd) ([]any, error) {
	if c.amount < 0 {
		return nil, errors.New("negative deposit")
	}
	if c.amount == 0 {
		return nil, nil
	}
	return []any{orderAdded{Delta: c.amount}}, nil
}

type typedNilSnapshotStore struct{ snapshot.Store }
type typedNilSnapshotCodec struct{ snapshot.Codec[accountState] }
type typedNilTransformer struct{ snapshot.PayloadTransformer }

func baseSnapAdapterCfg(
	db *spyDB,
	s *spyStore,
	snapStore snapshot.Store,
) postgres.SnapshotAdapterConfig[accountState, any] {
	return postgres.SnapshotAdapterConfig[accountState, any]{
		Decider: accountDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:         db,
			EventStore: s,
			StreamType: "Account",
			Codec:      jsonAccountCodec(),
		},
		Snapshot: postgres.SnapshotConfig[accountState]{
			Store:         snapStore,
			Codec:         snapshot.JSON[accountState](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(3),
		},
	}
}

func TestNewSnapshotAdapter_ValidationAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)
	var (
		nilDB          *spyDB
		nilSnapStore   *typedNilSnapshotStore
		nilSnapCodec   *typedNilSnapshotCodec
		nilTransformer *typedNilTransformer
	)

	cases := []struct {
		name      string
		mutate    func(*postgres.SnapshotAdapterConfig[accountState, any])
		wantErr   error
		errSubstr string
	}{
		{
			name:    "nil db",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.EventRepository.DB = nil },
			wantErr: postgres.ErrNilDB,
		},
		{
			name:    "typed-nil db",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.EventRepository.DB = nilDB },
			wantErr: postgres.ErrNilDB,
		},
		{
			name:    "nil event store",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.EventRepository.EventStore = nil },
			wantErr: postgres.ErrNilEventStore,
		},
		{
			name: "nil stream reader",
			mutate: func(c *postgres.SnapshotAdapterConfig[accountState, any]) {
				c.EventRepository.EventStore = splitEventStore{es: s}
			},
			wantErr: postgres.ErrNilStreamReader,
		},
		{
			name:    "blank stream type",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.EventRepository.StreamType = " " },
			wantErr: postgres.ErrEmptyStreamType,
		},
		{
			name:    "nil event codec",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.EventRepository.Codec = nil },
			wantErr: postgres.ErrNilCodec,
		},
		{
			name:    "nil decider initial",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Decider.Initial = nil },
			wantErr: persistence.ErrNilInitial,
		},
		{
			name:    "nil decider evolve",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Decider.Evolve = nil },
			wantErr: persistence.ErrNilEvolve,
		},
		{
			name:    "nil snapshot store",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Snapshot.Store = nil },
			wantErr: postgres.ErrNilSnapshotStore,
		},
		{
			name:    "typed-nil snapshot store",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Snapshot.Store = nilSnapStore },
			wantErr: postgres.ErrNilSnapshotStore,
		},
		{
			name:    "nil snapshot codec",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Snapshot.Codec = nil },
			wantErr: postgres.ErrNilSnapshotCodec,
		},
		{
			name:    "typed-nil snapshot codec",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Snapshot.Codec = nilSnapCodec },
			wantErr: postgres.ErrNilSnapshotCodec,
		},
		{
			name:    "nil snapshot policy",
			mutate:  func(c *postgres.SnapshotAdapterConfig[accountState, any]) { c.Snapshot.Policy = nil },
			wantErr: postgres.ErrNilSnapshotPolicy,
		},
		{
			name: "nil transformer",
			mutate: func(c *postgres.SnapshotAdapterConfig[accountState, any]) {
				c.Snapshot.Transformers = []snapshot.PayloadTransformer{nil}
			},
			wantErr: postgres.ErrNilPayloadTransformer,
		},
		{
			name: "typed-nil transformer",
			mutate: func(c *postgres.SnapshotAdapterConfig[accountState, any]) {
				c.Snapshot.Transformers = []snapshot.PayloadTransformer{nilTransformer}
			},
			wantErr: postgres.ErrNilPayloadTransformer,
		},
		{
			name: "invalid schema version",
			mutate: func(c *postgres.SnapshotAdapterConfig[accountState, any]) {
				c.Snapshot.SchemaVersion = 0
			},
			errSubstr: "schema version must be >= 1",
		},
		{
			name: "invalid upcaster version",
			mutate: func(c *postgres.SnapshotAdapterConfig[accountState, any]) {
				c.Snapshot.SchemaVersion = 1
				c.Snapshot.Upcasters = map[int]snapshot.Upcaster{1: func(_ int, b []byte) ([]byte, error) { return b, nil }}
			},
			errSubstr: "upcaster source schema version",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := baseSnapAdapterCfg(db, s, snapStore)
			tc.mutate(&cfg)
			_, err := postgres.NewSnapshotAdapter(cfg)
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("narrow err = %v, want %v", err, tc.wantErr)
			}
			if tc.errSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
				t.Fatalf("narrow err = %v, want substr %q", err, tc.errSubstr)
			}

			if tc.name != "nil db" && tc.name != "typed-nil db" {
				txCfg := postgres.TxSnapshotAdapterConfig[accountState, any]{
					Decider: cfg.Decider,
					TxEventRepository: postgres.TxEventRepositoryConfig[any]{
						EventStore: cfg.EventRepository.EventStore,
						Reader:     cfg.EventRepository.Reader,
						Codec:      cfg.EventRepository.Codec,
						StreamType: cfg.EventRepository.StreamType,
					},
					Snapshot: cfg.Snapshot,
				}
				_, txErr := postgres.NewTxSnapshotAdapter(txCfg)
				if tc.wantErr != nil && !errors.Is(txErr, tc.wantErr) {
					t.Fatalf("tx err = %v, want %v", txErr, tc.wantErr)
				}
				if tc.errSubstr != "" && (txErr == nil || !strings.Contains(txErr.Error(), tc.errSubstr)) {
					t.Fatalf("tx err = %v, want substr %q", txErr, tc.errSubstr)
				}
			}
		})
	}

	// Defensive copy of Upcasters and Transformers against caller mutation after construction.
	upcasters := map[int]snapshot.Upcaster{
		1: func(_ int, b []byte) ([]byte, error) { return b, nil },
	}
	transformers := []snapshot.PayloadTransformer{snapshot.Gzip()}
	cfg := baseSnapAdapterCfg(db, s, snapStore)
	cfg.Snapshot.SchemaVersion = 2
	cfg.Snapshot.Upcasters = upcasters
	cfg.Snapshot.Transformers = transformers
	adapter, err := postgres.NewSnapshotAdapter(cfg)
	if err != nil {
		t.Fatalf("NewSnapshotAdapter: %v", err)
	}
	upcasters[1] = nil
	transformers[0] = nil

	h := persistence.MustNewCommandHandler(adapter, depositID, decideDeposit)
	if err := h.Handle(context.Background(), depositCmd{id: "acc-copy", amount: 5}); err != nil {
		t.Fatalf("Handle after caller slice/map mutation: %v", err)
	}
}

func TestSnapshotAdapter_NarrowLifecycleContextDecodeAndDeltaReplay(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)

	baseCodec := jsonAccountCodec()
	var seenDecodeCtxVals []string
	var evolveCalls int
	decider := persistence.Decider[accountState, any]{
		Initial: accountDecider().Initial,
		Evolve: func(st accountState, ev any) accountState {
			evolveCalls++
			return accountDecider().Evolve(st, ev)
		},
	}

	var projSeen []store.PersistedEvent
	cfg := baseSnapAdapterCfg(db, s, snapStore)
	cfg.Decider = decider
	cfg.EventRepository.Codec = postgres.FuncEventCodec[any]{
		EncodeFunc: baseCodec.Encode,
		DecodeFunc: func(ctx context.Context, pe store.PersistedEvent) (any, error) {
			if v, ok := ctx.Value(ctxKey{}).(string); ok {
				seenDecodeCtxVals = append(seenDecodeCtxVals, v)
			}
			return baseCodec.Decode(ctx, pe)
		},
	}
	cfg.EventRepository.Projections = []postgres.Projection{
		postgres.ProjectionFunc(func(_ context.Context, _ pgx.Tx, ev store.PersistedEvent) error {
			projSeen = append(projSeen, ev)
			return nil
		}),
	}
	cfg.Snapshot.Version = func(st accountState) int64 { return st.Version }

	adapter, err := postgres.NewSnapshotAdapter(cfg)
	if err != nil {
		t.Fatalf("NewSnapshotAdapter: %v", err)
	}

	var activeTxInDecide bool
	h := persistence.MustNewCommandHandler(adapter, depositID, func(st accountState, c depositCmd) ([]any, error) {
		db.recordTimeline("decide")
		if _, ok := postgres.TxFromContext(context.Background()); ok {
			activeTxInDecide = true
		}
		return decideDeposit(st, c)
	})

	ctx := context.WithValue(context.Background(), ctxKey{}, "req-ctx")
	// 1 & 2: below snapshot threshold (3) -> zero snapshot Put and zero wasted nextState fold.
	if err := h.Handle(ctx, depositCmd{id: "acc-1", amount: 10}); err != nil {
		t.Fatalf("cmd 1: %v", err)
	}
	if snapStore.putCalls != 0 || evolveCalls != 0 {
		t.Fatalf("after cmd 1: putCalls=%d evolveCalls=%d, want 0/0", snapStore.putCalls, evolveCalls)
	}
	wantTimeline := []string{
		"read_tx:begin", "snapshot:get", "store:read", "read_tx:commit",
		"decide",
		"write_tx:begin", "store:append", "write_tx:commit",
	}
	if !slices.Equal(db.snapshotTimeline(), wantTimeline) {
		t.Fatalf("timeline = %v, want %v", db.snapshotTimeline(), wantTimeline)
	}

	// Zero-event decision triggers only read_tx, zero write_tx.
	if err := h.Handle(ctx, depositCmd{id: "acc-1", amount: 0}); err != nil {
		t.Fatalf("cmd noop: %v", err)
	}
	if db.txCount() != 3 { // 2 from cmd 1 + 1 read_tx from noop
		t.Fatalf("txCount after noop = %d, want 3", db.txCount())
	}

	// 2nd write -> version 2, still no snapshot.
	if err := h.Handle(ctx, depositCmd{id: "acc-1", amount: 20}); err != nil {
		t.Fatalf("cmd 2: %v", err)
	}
	if snapStore.putCalls != 0 {
		t.Fatalf("after cmd 2: putCalls=%d, want 0", snapStore.putCalls)
	}

	// 3rd write -> crosses EveryNEvents(3) -> saves snapshot at v3 via savepoint commit.
	if err := h.Handle(ctx, depositCmd{id: "acc-1", amount: 30}); err != nil {
		t.Fatalf("cmd 3: %v", err)
	}
	if snapStore.putCalls != 1 {
		t.Fatalf("after cmd 3: putCalls=%d, want 1", snapStore.putCalls)
	}
	snapRow, ok := snapStore.latest("Account", "acc-1", 1)
	if !ok || snapRow.StreamVersion != 3 {
		t.Fatalf("saved snap = (%+v, %v), want StreamVersion=3", snapRow, ok)
	}

	// 4th write loads snapshot v3 (zero events replayed) + appends v4, leaving snapshot at v3.
	if err := h.Handle(ctx, depositCmd{id: "acc-1", amount: 40}); err != nil {
		t.Fatalf("cmd 4: %v", err)
	}
	// 5th write reloads snapshot v3 + replays delta v4 (verifying EventCodec.Decode received ctx!).
	seenDecodeCtxVals = nil
	var sawBalanceBefore int
	hCheck := persistence.MustNewCommandHandler(adapter, depositID, func(st accountState, c depositCmd) ([]any, error) {
		sawBalanceBefore = st.Balance
		return decideDeposit(st, c)
	})
	if err := hCheck.Handle(ctx, depositCmd{id: "acc-1", amount: 50}); err != nil {
		t.Fatalf("cmd 5: %v", err)
	}
	if sawBalanceBefore != 100 {
		t.Fatalf("loaded balance from snap v3 + delta v4 = %d, want 100", sawBalanceBefore)
	}
	if !slices.Equal(seenDecodeCtxVals, []string{"req-ctx"}) {
		t.Fatalf("EventCodec.Decode ctx values = %v, want [req-ctx]", seenDecodeCtxVals)
	}
	if len(projSeen) != 5 || activeTxInDecide {
		t.Fatalf("projSeen=%d activeTxInDecide=%v", len(projSeen), activeTxInDecide)
	}

	// Explicit snapshot.Never() loads existing snapshots without writing new ones.
	neverCfg := cfg
	neverCfg.Snapshot.Policy = snapshot.Never()
	neverAdapter, err := postgres.NewSnapshotAdapter(neverCfg)
	if err != nil {
		t.Fatalf("neverAdapter: %v", err)
	}
	putsBefore := snapStore.putCalls
	if err := persistence.MustNewCommandHandler(neverAdapter, depositID, decideDeposit).
		Handle(ctx, depositCmd{id: "acc-1", amount: 1}); err != nil {
		t.Fatalf("never handle: %v", err)
	}
	if snapStore.putCalls != putsBefore {
		t.Fatalf("Never policy should not save snapshots")
	}
}

func TestSnapshotAdapter_UpcasterFallbackTransformersAndStrictNonReentrancy(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)
	ctx := context.Background()

	// Seed v1..v3 events + compressed schema v1 snapshot with legacy field name.
	narrowEV, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Account", Codec: jsonAccountCodec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := narrowEV.Append(ctx, "acc-up", 0, []any{
		orderAdded{Delta: 10},
		orderAdded{Delta: 15},
		orderAdded{Delta: 25},
	}); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	gzip := snapshot.Gzip()
	v1Compressed, err := gzip.Transform(ctx, []byte(`{"id":"acc-up","legacy_balance":50,"version":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := snapStore.Put(ctx, nil, &snapshot.Snapshot{
		StreamType:    "Account",
		StreamID:      "acc-up",
		StreamVersion: 3,
		SchemaVersion: 1,
		Payload:       v1Compressed,
	}); err != nil {
		t.Fatal(err)
	}

	var rejectedReasons []string
	cfg := baseSnapAdapterCfg(db, s, snapStore)
	cfg.Snapshot.SchemaVersion = 2
	cfg.Snapshot.Policy = snapshot.EveryNEvents(4)
	cfg.Snapshot.Transformers = []snapshot.PayloadTransformer{gzip}
	cfg.Snapshot.Upcasters = map[int]snapshot.Upcaster{
		1: func(_ int, raw []byte) ([]byte, error) {
			return []byte(strings.ReplaceAll(string(raw), `"legacy_balance"`, `"balance"`)), nil
		},
	}
	cfg.Snapshot.OnSnapshotRejected = func(_ context.Context, _ string, reason string, _ error) {
		rejectedReasons = append(rejectedReasons, reason)
	}

	adapter, err := postgres.NewSnapshotAdapter(cfg)
	if err != nil {
		t.Fatalf("NewSnapshotAdapter v2: %v", err)
	}

	var seenBal int
	h := persistence.MustNewCommandHandler(adapter, depositID, func(st accountState, c depositCmd) ([]any, error) {
		seenBal = st.Balance
		return decideDeposit(st, c)
	})
	if err := h.Handle(ctx, depositCmd{id: "acc-up", amount: 5}); err != nil {
		t.Fatalf("Handle upcasted: %v", err)
	}
	if seenBal != 50 {
		t.Fatalf("upcasted snapshot balance = %d, want 50", seenBal)
	}

	// Corrupt v1 snapshot: FailOnCorruptSnapshot=true aborts loudly before decide;
	// FailOnCorruptSnapshot=false falls back to full replay, triggers callback, and heals v2 snapshot.
	snapStore.snaps[streamKey("Account", "acc-up")][1] = snapshot.Snapshot{
		StreamType:    "Account",
		StreamID:      "acc-up",
		StreamVersion: 3,
		SchemaVersion: 1,
		Payload:       []byte("not-gzip"),
	}
	strictCfg := cfg
	strictCfg.Snapshot.FailOnCorruptSnapshot = true
	strictAdapter, err := postgres.NewSnapshotAdapter(strictCfg)
	if err != nil {
		t.Fatal(err)
	}
	strictH := persistence.MustNewCommandHandler(strictAdapter, depositID, decideDeposit)
	if err := strictH.Handle(ctx, depositCmd{id: "acc-up", amount: 5}); err == nil {
		t.Fatal("expected error with FailOnCorruptSnapshot=true on corrupt snapshot")
	}

	if err := h.Handle(ctx, depositCmd{id: "acc-up", amount: 5}); err != nil {
		t.Fatalf("Handle corrupt fallback: %v", err)
	}
	if seenBal != 55 || len(rejectedReasons) == 0 {
		t.Fatalf("fallback balance=%d reasons=%v, want 55 and non-empty", seenBal, rejectedReasons)
	}
	if healed, ok := snapStore.latest("Account", "acc-up", 2); !ok || healed.StreamVersion != 5 {
		t.Fatalf("expected healed schema v2 snapshot at StreamVersion 5, got (%+v, %v)", healed, ok)
	}
	if err := strictH.Handle(ctx, depositCmd{id: "acc-up", amount: 5}); err != nil {
		t.Fatalf("strict mode should succeed once v2 snapshot healed: %v", err)
	}

	// Strict non-reentrancy: mixing narrow SnapshotAdapter inside NewTxMiddleware fails with ErrTxAlreadyActive.
	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(db)))
	if err != nil {
		t.Fatal(err)
	}
	command.MustRegister(bus, h)
	if err := bus.Dispatch(ctx, depositCmd{id: "acc-up", amount: 1}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("narrow inside TxMiddleware err = %v, want ErrTxAlreadyActive", err)
	}

	// Context carrying AdoptTx fails before opening narrow read tx.
	adoptedCtx, err := postgres.AdoptTx(ctx, &spyTx{db: db})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Handle(adoptedCtx, depositCmd{id: "acc-up", amount: 1}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("AdoptTx + narrow err = %v, want ErrTxAlreadyActive", err)
	}
}

func TestSnapshotAdapter_SavepointIsolationErrorsAndOCCRetry(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)
	snapStore.putErr = errors.New("snapshot put transient failure")
	logger := &spyLogger{}
	ctx := context.Background()

	var (
		saveErrCount int
		saveErrVer   int64
	)
	cfg := baseSnapAdapterCfg(db, s, snapStore)
	cfg.Snapshot.Policy = snapshot.EveryNEvents(1)
	cfg.Snapshot.Logger = logger
	cfg.Snapshot.OnSnapshotSaveError = func(_ context.Context, _ string, ver int64, err error) {
		if err != nil {
			saveErrCount++
			saveErrVer = ver
		}
	}

	bestEffortAdapter, err := postgres.NewSnapshotAdapter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	hBestEffort := persistence.MustNewCommandHandler(bestEffortAdapter, depositID, decideDeposit)
	if err := hBestEffort.Handle(ctx, depositCmd{id: "acc-sp", amount: 10}); err != nil {
		t.Fatalf("best-effort snapshot failure should commit events, got err=%v", err)
	}
	if saveErrCount != 1 || saveErrVer != 1 || len(logger.errors) != 1 {
		t.Fatalf("saveErrCount=%d ver=%d logs=%d, want 1/1/1", saveErrCount, saveErrVer, len(logger.errors))
	}
	if got := len(s.streams[streamKey("Account", "acc-sp")]); got != 1 {
		t.Fatalf("events persisted = %d, want 1", got)
	}
	// Outer write tx committed while savepoint rolled back cleanly.
	writeTx := db.txs[len(db.txs)-1]
	if !writeTx.committed || writeTx.rolledBack || !writeTx.subTxs[0].rolledBack {
		t.Fatalf("unexpected tx/subTx states: committed=%v rolledBack=%v subRolledBack=%v",
			writeTx.committed, writeTx.rolledBack, writeTx.subTxs[0].rolledBack)
	}

	// Best-effort NEVER swallows canceled contexts or savepoint begin/rollback failures.
	snapStore.putErr = context.Canceled
	if err := hBestEffort.Handle(ctx, depositCmd{id: "acc-sp", amount: 5}); !errors.Is(err, context.Canceled) {
		t.Fatalf("context.Canceled from Put should fail loudly, got %v", err)
	}

	snapStore.putErr = errors.New("put boom")
	db.nextSubBeginErr = errors.New("sp begin failed")
	if err := hBestEffort.Handle(ctx, depositCmd{id: "acc-sp", amount: 5}); err == nil {
		t.Fatal("expected subBeginErr to fail tx")
	}

	db.nextSubRollbackEr = errors.New("sp rollback failed")
	if err := hBestEffort.Handle(ctx, depositCmd{id: "acc-sp", amount: 5}); err == nil {
		t.Fatal("expected subRollbackErr to fail tx")
	}

	// Strict FailOnSnapshotSaveError=true rolls back outer write tx cleanly.
	atomicCfg := cfg
	atomicCfg.Snapshot.FailOnSnapshotSaveError = true
	atomicAdapter, err := postgres.NewSnapshotAdapter(atomicCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.MustNewCommandHandler(atomicAdapter, depositID, decideDeposit).
		Handle(ctx, depositCmd{id: "acc-atomic", amount: 10}); err == nil {
		t.Fatal("expected atomic mode error on Put failure")
	}

	// OCC conflict wraps persistence.ErrConcurrencyConflict & store.ErrOptimisticConcurrency,
	// skips snapshot save on the conflicted attempt, and retries cleanly via RetryMiddleware.
	snapStore.putErr = nil
	s.failOnceOCC = 1
	beforePuts := snapStore.putCalls
	bus, err := command.NewBus(command.WithMiddleware(command.RetryMiddleware(command.RetryConfig{
		MaxAttempts: 2,
		RetryIf:     postgres.IsConcurrencyConflict,
	})))
	if err != nil {
		t.Fatal(err)
	}
	command.MustRegister(bus, persistence.MustNewCommandHandler(atomicAdapter, depositID, decideDeposit))
	if err := bus.Dispatch(ctx, depositCmd{id: "acc-occ", amount: 7}); err != nil {
		t.Fatalf("OCC retry dispatch failed: %v", err)
	}
	if snapStore.putCalls != beforePuts+1 {
		t.Fatalf("expected exactly 1 snapshot Put after OCC retry, got delta=%d", snapStore.putCalls-beforePuts)
	}
}

func TestTxSnapshotAdapter_WideMiddlewareAndGuards(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)
	ctx := context.Background()

	adapter, err := postgres.NewTxSnapshotAdapter(postgres.TxSnapshotAdapterConfig[accountState, any]{
		Decider: accountDecider(),
		TxEventRepository: postgres.TxEventRepositoryConfig[any]{
			EventStore: s,
			StreamType: "Account",
			Codec:      jsonAccountCodec(),
		},
		Snapshot: postgres.SnapshotConfig[accountState]{
			Store:         snapStore,
			Codec:         snapshot.JSON[accountState](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(1),
		},
	})
	if err != nil {
		t.Fatalf("NewTxSnapshotAdapter: %v", err)
	}

	h := persistence.MustNewCommandHandler(adapter, depositID, decideDeposit)
	if err := h.Handle(ctx, depositCmd{id: "acc-wide", amount: 15}); !errors.Is(err, postgres.ErrMissingTx) {
		t.Fatalf("outside tx err = %v, want ErrMissingTx", err)
	}

	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(db)))
	if err != nil {
		t.Fatal(err)
	}
	command.MustRegister(bus, h)
	if err := bus.Dispatch(ctx, depositCmd{id: "acc-wide", amount: 15}); err != nil {
		t.Fatalf("wide snapshot dispatch: %v", err)
	}
	if db.txCount() != 1 || snapStore.putCalls != 1 {
		t.Fatalf("txCount=%d putCalls=%d, want 1/1", db.txCount(), snapStore.putCalls)
	}
	if _, ok := snapStore.latest("Account", "acc-wide", 1); !ok {
		t.Fatal("expected snapshot written inside wide tx")
	}

	// Zero-event inside active wide tx skips append and snapshot.
	if err := bus.Dispatch(ctx, depositCmd{id: "acc-wide", amount: 0}); err != nil {
		t.Fatalf("wide noop: %v", err)
	}
	if snapStore.putCalls != 1 {
		t.Fatalf("wide noop putCalls = %d, want 1", snapStore.putCalls)
	}
}

func TestSnapshotAdapter_LoadDecodeEncodeAndDecideErrors(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	snapStore := newSpySnapshotStore(db)
	ctx := context.Background()

	// Seed 1 valid event so Decode error runs on replay.
	seedRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Account", Codec: jsonAccountCodec(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := seedRepo.Append(ctx, "acc-err", 0, []any{orderAdded{Delta: 5}}); err != nil {
		t.Fatal(err)
	}

	decodeErr := errors.New("decode failure")
	encodeErr := errors.New("encode failure")
	var failDecode, failEncode bool

	cfg := baseSnapAdapterCfg(db, s, snapStore)
	baseCodec := jsonAccountCodec()
	cfg.EventRepository.Codec = postgres.FuncEventCodec[any]{
		EncodeFunc: func(c context.Context, st, id string, ev any) (store.Event, error) {
			if failEncode {
				return store.Event{}, encodeErr
			}
			return baseCodec.Encode(c, st, id, ev)
		},
		DecodeFunc: func(c context.Context, pe store.PersistedEvent) (any, error) {
			if failDecode {
				return nil, decodeErr
			}
			return baseCodec.Decode(c, pe)
		},
	}

	adapter, err := postgres.NewSnapshotAdapter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := persistence.MustNewCommandHandler(adapter, depositID, decideDeposit)

	// 1. Load error from snapshot store.
	snapStore.getErr = errors.New("snapshot get failed")
	if err := h.Handle(ctx, depositCmd{id: "acc-err", amount: 10}); err == nil {
		t.Fatal("expected load error when snapStore.Get fails")
	}
	snapStore.getErr = nil

	// 2. EventCodec.Decode error during delta replay.
	failDecode = true
	if err := h.Handle(ctx, depositCmd{id: "acc-err", amount: 10}); !errors.Is(err, decodeErr) {
		t.Fatalf("decode err = %v, want %v", err, decodeErr)
	}
	failDecode = false

	// 3. Domain decide returning error skips write tx.
	txBeforeDecideErr := db.txCount()
	if err := h.Handle(ctx, depositCmd{id: "acc-err", amount: -5}); err == nil {
		t.Fatal("expected negative deposit error")
	}
	if db.txCount() != txBeforeDecideErr+1 {
		t.Fatalf("expected only read_tx before decide err, got delta=%d", db.txCount()-txBeforeDecideErr)
	}

	// 4. EventCodec.Encode error skips write tx.
	failEncode = true
	txBeforeEncodeErr := db.txCount()
	if err := h.Handle(ctx, depositCmd{id: "acc-err", amount: 5}); !errors.Is(err, encodeErr) {
		t.Fatalf("encode err = %v, want %v", err, encodeErr)
	}
	if db.txCount() != txBeforeEncodeErr+1 {
		t.Fatalf("expected only read_tx before encode err, got delta=%d", db.txCount()-txBeforeEncodeErr)
	}
}
