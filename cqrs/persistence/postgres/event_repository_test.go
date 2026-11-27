package postgres_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
)

func TestNewEventRepository_Validation(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	codec := jsonAccountCodec()

	var (
		typedNilDB     *spyDB
		typedNilStore  *spyStore
		typedNilReader *spyStore
		typedNilProj   *spyProjection
		typedNilBatch  *spyBatchProjection
	)

	for _, tc := range []struct {
		name    string
		cfg     postgres.EventRepositoryConfig[any]
		wantErr error
	}{
		{name: "nil db", cfg: postgres.EventRepositoryConfig[any]{EventStore: s, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilDB},
		{name: "typed-nil db", cfg: postgres.EventRepositoryConfig[any]{DB: typedNilDB, EventStore: s, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilDB},
		{name: "nil store", cfg: postgres.EventRepositoryConfig[any]{DB: db, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilEventStore},
		{name: "typed-nil store", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: typedNilStore, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilEventStore},
		{name: "store not reader", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: splitEventStore{es: s}, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilStreamReader},
		{name: "typed-nil reader", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, Reader: typedNilReader, StreamType: "Order", Codec: codec}, wantErr: postgres.ErrNilStreamReader},
		{name: "empty stream type", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "  \t ", Codec: codec}, wantErr: postgres.ErrEmptyStreamType},
		{name: "nil codec", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order"}, wantErr: postgres.ErrNilCodec},
		{name: "partial codec", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: postgres.FuncEventCodec[any]{EncodeFunc: codec.Encode}}, wantErr: postgres.ErrNilCodec},
		{name: "nil projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, Projections: []postgres.Projection{nil}}, wantErr: postgres.ErrNilProjection},
		{name: "typed-nil projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, Projections: []postgres.Projection{typedNilProj}}, wantErr: postgres.ErrNilProjection},
		{name: "nil batch projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, BatchProjections: []postgres.BatchProjection{nil}}, wantErr: postgres.ErrNilProjection},
		{name: "typed-nil batch projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, BatchProjections: []postgres.BatchProjection{typedNilBatch}}, wantErr: postgres.ErrNilProjection},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo, err := postgres.NewEventRepository(tc.cfg)
			if repo != nil || !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewEventRepository = (%v, %v), want (nil, %v)", repo, err, tc.wantErr)
			}
		})
	}
}

func TestEventRepository_LifecycleMetadataAndProjections(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	fixedTime := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	fixedID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	var projected []string
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: splitEventStore{es: s},
		Reader:     s,
		StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: func(_ context.Context, _, _ string, ev any) (store.Event, error) {
				return jsonAccountCodec().Encode(context.Background(), "", "", ev)
			},
			DecodeFunc: jsonAccountCodec().Decode,
		},
		BatchProjections: []postgres.BatchProjection{
			postgres.BatchProjectionFunc(func(_ context.Context, tx pgx.Tx, events []store.PersistedEvent) error {
				if tx == nil {
					t.Fatal("expected active tx in batch projection")
				}
				projected = append(projected, "batch:"+events[0].StreamID)
				return nil
			}),
		},
		Projections: []postgres.Projection{
			postgres.ProjectionFunc(func(_ context.Context, tx pgx.Tx, ev store.PersistedEvent) error {
				if tx == nil {
					t.Fatal("expected active tx in projection")
				}
				projected = append(projected, "single:"+ev.EventID.String())
				return nil
			}),
		},
		Clock: func() time.Time { return fixedTime },
		IDGen: func() uuid.UUID { return fixedID },
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	// Guard checks on Load & Append.
	if _, _, loadErr := repo.Load(nil, "ord-1"); !errors.Is(loadErr, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("Load(nil) err = %v", loadErr)
	}
	if _, _, loadErr := repo.Load(context.Background(), "  "); !errors.Is(loadErr, persistence.ErrEmptyTargetID) {
		t.Fatalf("Load(blank) err = %v", loadErr)
	}
	if appErr := repo.Append(nil, "ord-1", 0, nil); !errors.Is(appErr, persistence.ErrNilContext) { //nolint:staticcheck // SA1012: testing nil ctx guard
		t.Fatalf("Append(nil ctx) err = %v", appErr)
	}
	if appErr := repo.Append(context.Background(), " ", 0, nil); !errors.Is(appErr, persistence.ErrEmptyTargetID) {
		t.Fatalf("Append(blank) err = %v", appErr)
	}
	if appErr := repo.Append(context.Background(), "ord-1", -1, nil); !errors.Is(appErr, persistence.ErrInvalidVersion) {
		t.Fatalf("Append(neg version) err = %v", appErr)
	}
	if appErr := repo.Append(context.Background(), "ord-1", 0, nil); appErr != nil {
		t.Fatalf("Append(empty) err = %v, want nil", appErr)
	}
	if db.txCount() != 0 {
		t.Fatalf("empty Append opened %d txs, want 0", db.txCount())
	}

	// Verify Append auto-fills metadata + observability and executes narrow tx.
	ctx := cqrs.WithObservability(context.Background(), cqrs.Observability{
		CommandID:   "cmd-1",
		TraceID:     "trace-1",
		CausationID: "outer-cause",
	})
	if err := repo.Append(ctx, "ord-1", 0, []any{orderAdded{Delta: 50}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	loaded, ver, err := repo.Load(ctx, "ord-1")
	if err != nil || ver != 1 || len(loaded) != 1 {
		t.Fatalf("Load = (%v, %d, %v), want 1 event at ver 1", loaded, ver, err)
	}
	stored := s.streams[streamKey("Order", "ord-1")][0]
	if stored.StreamType != "Order" || stored.StreamID != "ord-1" ||
		stored.EventID != fixedID || stored.CreatedAt != fixedTime ||
		stored.TraceID.String != "trace-1" || stored.CausationID.String != "cmd-1" {
		t.Fatalf("unexpected stamped event metadata: %+v", stored)
	}
	if !slices.Equal(projected, []string{"batch:ord-1", "single:" + fixedID.String()}) {
		t.Fatalf("projected = %v, want batch then single", projected)
	}
	wantTimeline := []string{
		"write_tx:begin", "store:append", "write_tx:commit",
		"read_tx:begin", "store:read", "read_tx:commit",
	}
	if !slices.Equal(db.snapshotTimeline(), wantTimeline) {
		t.Fatalf("timeline = %v, want %v", db.snapshotTimeline(), wantTimeline)
	}
}

func TestEventRepository_StreamMismatchAndProjectionError(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	badCodec := postgres.FuncEventCodec[any]{
		EncodeFunc: func(_ context.Context, _, _ string, _ any) (store.Event, error) {
			return store.Event{StreamType: "WrongStream", StreamID: "ord-1", EventType: "X", Payload: []byte(`{}`)}, nil
		},
		DecodeFunc: jsonAccountCodec().Decode,
	}

	badRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      badCodec,
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}
	if err := badRepo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, postgres.ErrStreamMismatch) {
		t.Fatalf("wrong streamType err = %v, want ErrStreamMismatch", err)
	}
	if db.txCount() != 0 {
		t.Fatalf("codec streamType mismatch opened %d txs, want 0", db.txCount())
	}

	wrongIDRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: func(_ context.Context, _, _ string, _ any) (store.Event, error) {
				return store.Event{StreamType: "Order", StreamID: "wrong-id", EventType: "X", Payload: []byte(`{}`)}, nil
			},
			DecodeFunc: jsonAccountCodec().Decode,
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository wrongID: %v", err)
	}
	if err := wrongIDRepo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, postgres.ErrStreamMismatch) {
		t.Fatalf("wrong streamID err = %v, want ErrStreamMismatch", err)
	}
	if db.txCount() != 0 {
		t.Fatalf("codec streamID mismatch opened %d txs, want 0", db.txCount())
	}

	readFail := errors.New("read boom")
	s.readErr = readFail
	if _, _, err := wrongIDRepo.Load(context.Background(), "ord-1"); !errors.Is(err, readFail) {
		t.Fatalf("Load readErr = %v, want %v", err, readFail)
	}
	s.readErr = nil

	batchErr := errors.New("batch projection failed")
	batchFailRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      jsonAccountCodec(),
		BatchProjections: []postgres.BatchProjection{
			postgres.BatchProjectionFunc(func(context.Context, pgx.Tx, []store.PersistedEvent) error {
				return batchErr
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository batchFail: %v", err)
	}
	if err := batchFailRepo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, batchErr) {
		t.Fatalf("batch projection failure err = %v, want %v", err, batchErr)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatalf("expected batch projection error to roll back write tx, got: %+v", db.txs[db.txCount()-1])
	}

	projErr := errors.New("projection failed")
	projRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      jsonAccountCodec(),
		Projections: []postgres.Projection{
			postgres.ProjectionFunc(func(context.Context, pgx.Tx, store.PersistedEvent) error {
				return projErr
			}),
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}
	if err := projRepo.Append(context.Background(), "ord-proj", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, projErr) {
		t.Fatalf("projection failure err = %v, want %v", err, projErr)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatalf("expected projection error to roll back write tx, got: %+v", db.txs[db.txCount()-1])
	}
}

type spyProjection struct{}

func (*spyProjection) Handle(context.Context, pgx.Tx, store.PersistedEvent) error { return nil }

type spyBatchProjection struct{}

func (*spyBatchProjection) Handle(context.Context, pgx.Tx, []store.PersistedEvent) error {
	return nil
}

func TestFromEventRepository_EndToEndWithNarrowTx(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	adapter := persistence.FromEventRepository(repo, accountDecider())
	h := persistence.MustNewCommandHandler(
		adapter,
		func(delta int) string { return "ord-1" },
		func(state accountState, delta int) ([]any, error) {
			db.recordTimeline("decide")
			return []any{orderAdded{Delta: delta}}, nil
		},
	)

	bus, err := command.NewBus(command.WithMiddleware(command.ObservabilityMiddleware()))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, h)

	if err := bus.Dispatch(context.Background(), 25); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	wantTimeline := []string{
		"read_tx:begin", "store:read", "read_tx:commit",
		"decide",
		"write_tx:begin", "store:append", "write_tx:commit",
	}
	if !slices.Equal(db.snapshotTimeline(), wantTimeline) {
		t.Fatalf("timeline = %v, want %v", db.snapshotTimeline(), wantTimeline)
	}
}

func TestFromEventRepository_WideTxReuseAndDecodeError(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	adapter := persistence.FromEventRepository(repo, accountDecider())
	h := persistence.MustNewCommandHandler(
		adapter,
		func(delta int) string { return "ord-wide" },
		func(state accountState, delta int) ([]any, error) {
			db.recordTimeline("decide")
			return []any{orderAdded{Delta: delta}}, nil
		},
	)

	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(db)))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, h)

	if dispatchErr := bus.Dispatch(context.Background(), 10); dispatchErr != nil {
		t.Fatalf("Dispatch: %v", dispatchErr)
	}
	wantTimeline := []string{
		"write_tx:begin",
		"store:read",
		"decide",
		"store:append",
		"write_tx:commit",
	}
	if !slices.Equal(db.snapshotTimeline(), wantTimeline) {
		t.Fatalf("wide timeline = %v, want %v", db.snapshotTimeline(), wantTimeline)
	}

	// Verify decode errors abort Load cleanly.
	decodeFail := errors.New("corrupt json")
	badDecRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: jsonAccountCodec().Encode,
			DecodeFunc: func(context.Context, store.PersistedEvent) (any, error) {
				return nil, decodeFail
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}
	if _, _, loadErr := badDecRepo.Load(context.Background(), "ord-wide"); !errors.Is(loadErr, decodeFail) {
		t.Fatalf("Load decode error = %v, want %v", loadErr, decodeFail)
	}
}

type sqlStateRepo struct {
	db      postgres.TxBeginner
	records map[string]accountState
}

func (r *sqlStateRepo) Load(ctx context.Context, id string) (accountState, int64, error) {
	var (
		state accountState
		ver   int64
		found bool
	)
	err := postgres.InReadTx(ctx, r.db, func(_ context.Context, _ pgx.Tx) error {
		st, ok := r.records[id]
		if !ok {
			return nil
		}
		state = st
		ver = st.Version
		found = true
		return nil
	})
	if err != nil {
		return accountState{}, 0, err
	}
	if !found {
		return accountState{}, 0, persistence.ErrNotFound
	}
	return state, ver, nil
}

func (r *sqlStateRepo) Save(ctx context.Context, id string, expectedVersion int64, state accountState) error {
	return postgres.InTx(ctx, r.db, func(_ context.Context, _ pgx.Tx) error {
		cur := r.records[id]
		if cur.Version != expectedVersion {
			return persistence.ErrConcurrencyConflict
		}
		state.Version = expectedVersion + 1
		r.records[id] = state
		return nil
	})
}

func TestStateRepository_InReadTxAndInTxNarrowByDefault(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	repo := &sqlStateRepo{db: db, records: make(map[string]accountState)}
	h := persistence.MustNewCommandHandler(
		persistence.FromStateRepository(repo, accountDecider()),
		func(delta int) string { return "st-1" },
		func(state accountState, delta int) ([]any, error) {
			db.recordTimeline("decide")
			return []any{orderAdded{Delta: delta}}, nil
		},
	)

	if err := h.Handle(context.Background(), 15); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	want := []string{
		"read_tx:begin", "read_tx:commit",
		"decide",
		"write_tx:begin", "write_tx:commit",
	}
	if !slices.Equal(db.snapshotTimeline(), want) {
		t.Fatalf("state repo timeline = %v, want %v", db.snapshotTimeline(), want)
	}
}
