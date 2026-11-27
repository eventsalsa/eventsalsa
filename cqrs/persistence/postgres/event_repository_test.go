package postgres_test

import (
	"context"
	"errors"
	"fmt"
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

func TestNewRepositories_ValidationAndSliceIsolation(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	codec := jsonAccountCodec()

	var (
		typedNilDB     *spyDB
		typedNilStore  *spyStore
		typedNilReader *spyStore
		typedNilCodec  *postgres.FuncEventCodec[any]
		typedNilProj   *spyProjection
		typedNilBatch  *spyBatchProjection
		zeroFuncCodec  postgres.FuncEventCodec[any]
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
		{name: "empty string stream type", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "", Codec: codec}, wantErr: postgres.ErrEmptyStreamType},
		{name: "blank stream type", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "  \t ", Codec: codec}, wantErr: postgres.ErrEmptyStreamType},
		{name: "nil codec", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order"}, wantErr: postgres.ErrNilCodec},
		{name: "zero FuncEventCodec", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: zeroFuncCodec}, wantErr: postgres.ErrNilCodec},
		{name: "typed-nil FuncEventCodec ptr", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: typedNilCodec}, wantErr: postgres.ErrNilCodec},
		{name: "partial codec", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: postgres.FuncEventCodec[any]{EncodeFunc: codec.Encode}}, wantErr: postgres.ErrNilCodec},
		{name: "nil projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, Projections: []postgres.Projection{nil}}, wantErr: postgres.ErrNilProjection},
		{name: "typed-nil projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, Projections: []postgres.Projection{typedNilProj}}, wantErr: postgres.ErrNilProjection},
		{name: "nil batch projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, BatchProjections: []postgres.BatchProjection{nil}}, wantErr: postgres.ErrNilProjection},
		{name: "typed-nil batch projection", cfg: postgres.EventRepositoryConfig[any]{DB: db, EventStore: s, StreamType: "Order", Codec: codec, BatchProjections: []postgres.BatchProjection{typedNilBatch}}, wantErr: postgres.ErrNilProjection},
	} {
		t.Run("narrow/"+tc.name, func(t *testing.T) {
			t.Parallel()
			repo, err := postgres.NewEventRepository(tc.cfg)
			if repo != nil || !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewEventRepository = (%v, %v), want (nil, %v)", repo, err, tc.wantErr)
			}
		})

		if errors.Is(tc.wantErr, postgres.ErrNilDB) {
			continue
		}
		t.Run("tx/"+tc.name, func(t *testing.T) {
			t.Parallel()
			repo, err := postgres.NewTxEventRepository(postgres.TxEventRepositoryConfig[any]{
				EventStore:       tc.cfg.EventStore,
				Reader:           tc.cfg.Reader,
				Codec:            tc.cfg.Codec,
				StreamType:       tc.cfg.StreamType,
				Projections:      tc.cfg.Projections,
				BatchProjections: tc.cfg.BatchProjections,
			})
			if repo != nil || !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewTxEventRepository = (%v, %v), want (nil, %v)", repo, err, tc.wantErr)
			}
		})
	}

	// Mutating projection slices after repository construction must not affect repository execution.
	projCalls := 0
	projs := []postgres.Projection{
		postgres.ProjectionFunc(func(context.Context, pgx.Tx, store.PersistedEvent) error {
			projCalls++
			return nil
		}),
	}
	batches := []postgres.BatchProjection{
		postgres.BatchProjectionFunc(func(context.Context, pgx.Tx, []store.PersistedEvent) error {
			projCalls++
			return nil
		}),
	}
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Iso", Codec: codec,
		Projections: projs, BatchProjections: batches,
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}
	projs[0] = nil
	batches[0] = nil
	if err := repo.Append(context.Background(), "iso-1", 0, []any{orderAdded{Delta: 1}}); err != nil {
		t.Fatalf("Append after slice mutation: %v", err)
	}
	if projCalls != 2 {
		t.Fatalf("projCalls = %d, want 2", projCalls)
	}
}

func TestEventRepository_LifecycleMetadataAndMultiEventProjections(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	fixedTime := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	seq := 0
	genID := func() uuid.UUID {
		seq++
		return uuid.MustParse(fmt.Sprintf("11111111-1111-1111-1111-11111111111%d", seq))
	}

	var projected []string
	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: splitEventStore{es: s},
		Reader:     s,
		StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: func(_ context.Context, _, _ string, ev any) (store.Event, error) {
				base, encErr := jsonAccountCodec().Encode(context.Background(), "", "", ev)
				if encErr != nil {
					return store.Event{}, encErr
				}
				base.EventVersion = 0 // force default EventVersion = 1
				return base, nil
			},
			DecodeFunc: jsonAccountCodec().Decode,
		},
		BatchProjections: []postgres.BatchProjection{
			postgres.BatchProjectionFunc(func(_ context.Context, tx pgx.Tx, events []store.PersistedEvent) error {
				if tx == nil {
					t.Fatal("expected active tx in batch projection")
				}
				projected = append(projected, fmt.Sprintf("batch:%s:%d", events[0].StreamID, len(events)))
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
		IDGen: genID,
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	// Input guards & empty stream semantics.
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
		t.Fatalf("Append(nil slice) err = %v, want nil", appErr)
	}
	if appErr := repo.Append(context.Background(), "ord-1", 0, []any{}); appErr != nil {
		t.Fatalf("Append(empty slice) err = %v, want nil", appErr)
	}

	emptyLoaded, emptyVer, err := repo.Load(context.Background(), "missing")
	if err != nil || emptyVer != 0 || emptyLoaded != nil {
		t.Fatalf("Load(missing) = (%v, %d, %v), want (nil, 0, nil)", emptyLoaded, emptyVer, err)
	}

	// Append 2 events -> auto-fills metadata, runs BatchProjection once with 2 events and Projection twice.
	ctx := cqrs.WithObservability(context.Background(), cqrs.Observability{
		CommandID:   "cmd-1",
		TraceID:     "trace-1",
		CausationID: "outer-cause",
	})
	if err := repo.Append(ctx, "ord-1", 0, []any{orderAdded{Delta: 50}, orderAdded{Delta: 25}}); err != nil {
		t.Fatalf("Append(2): %v", err)
	}
	if err := repo.Append(cqrs.WithObservability(context.Background(), cqrs.Observability{
		CausationID: "fallback-cause-only",
	}), "ord-1", 2, []any{orderAdded{Delta: 5}}); err != nil {
		t.Fatalf("Append(ver=2): %v", err)
	}

	loaded, ver, err := repo.Load(ctx, "ord-1")
	if err != nil || ver != 3 || len(loaded) != 3 {
		t.Fatalf("Load = (%v, %d, %v), want 3 events at ver 3", loaded, ver, err)
	}
	wantLoaded := []any{orderAdded{Delta: 50}, orderAdded{Delta: 25}, orderAdded{Delta: 5}}
	if !slices.Equal(loaded, wantLoaded) {
		t.Fatalf("loaded = %#v, want %#v", loaded, wantLoaded)
	}

	persisted := s.streams[streamKey("Order", "ord-1")]
	if persisted[0].EventVersion != 1 ||
		persisted[0].StreamType != "Order" ||
		persisted[0].StreamID != "ord-1" ||
		persisted[0].CreatedAt != fixedTime ||
		persisted[0].TraceID.String != "trace-1" ||
		persisted[0].CausationID.String != "cmd-1" ||
		persisted[2].CausationID.String != "fallback-cause-only" {
		t.Fatalf("unexpected persisted metadata: ev0=%+v ev2=%+v", persisted[0], persisted[2])
	}
	if !slices.Equal(projected[:3], []string{
		"batch:ord-1:2",
		"single:11111111-1111-1111-1111-111111111111",
		"single:11111111-1111-1111-1111-111111111112",
	}) {
		t.Fatalf("unexpected multi-event projection trace: %v", projected)
	}
}

func TestEventRepository_PresetMetadataPreservationAndFailures(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	presetID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	presetTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: func(_ context.Context, _, _ string, ev any) (store.Event, error) {
				if _, ok := ev.(string); ok {
					return store.Event{}, errors.New("encode fail on string")
				}
				return store.Event{
					StreamType:   "Order",
					StreamID:     "ord-pre",
					EventID:      presetID,
					EventType:    "CustomEvent",
					EventVersion: 3,
					Payload:      []byte(`{}`),
					TraceID:      store.NullString{String: "codec-trace", Valid: true},
					CausationID:  store.NullString{String: "codec-cause", Valid: true},
					CreatedAt:    presetTime,
				}, nil
			},
			DecodeFunc: jsonAccountCodec().Decode,
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	// Failure on 2nd event encode opens zero write txs.
	if err := repo.Append(context.Background(), "ord-pre", 0, []any{orderAdded{Delta: 1}, "boom"}); err == nil {
		t.Fatal("expected encode error on 2nd event")
	}
	if db.txCount() != 0 {
		t.Fatalf("opened %d txs on encode error, want 0", db.txCount())
	}

	// Pre-populated codec metadata overrides defaults cleanly.
	ctx := cqrs.WithObservability(context.Background(), cqrs.Observability{
		CommandID: "ctx-cmd",
		TraceID:   "ctx-trace",
	})
	if err := repo.Append(ctx, "ord-pre", 0, []any{orderAdded{Delta: 1}}); err != nil {
		t.Fatalf("Append preset: %v", err)
	}
	stored := s.streams[streamKey("Order", "ord-pre")][0]
	if stored.EventID != presetID ||
		stored.EventVersion != 3 ||
		!stored.CreatedAt.Equal(presetTime) ||
		stored.TraceID.String != "codec-trace" ||
		stored.CausationID.String != "codec-cause" {
		t.Fatalf("preset metadata overwritten: %+v", stored)
	}

	// Codec mismatches (StreamType, StreamID, blank EventType) abort before opening write tx.
	for _, tc := range []struct {
		name    string
		se      store.Event
		wantErr error
	}{
		{name: "wrong streamType", se: store.Event{StreamType: "Wrong", StreamID: "ord-1", EventType: "X"}, wantErr: postgres.ErrStreamMismatch},
		{name: "wrong streamID", se: store.Event{StreamType: "Order", StreamID: "wrong-id", EventType: "X"}, wantErr: postgres.ErrStreamMismatch},
		{name: "blank eventType", se: store.Event{StreamType: "Order", StreamID: "ord-1", EventType: "  "}, wantErr: postgres.ErrEmptyEventType},
	} {
		txsBefore := db.txCount()
		badRepo, mkErr := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
			DB: db, EventStore: s, StreamType: "Order",
			Codec: postgres.FuncEventCodec[any]{
				EncodeFunc: func(context.Context, string, string, any) (store.Event, error) { return tc.se, nil },
				DecodeFunc: jsonAccountCodec().Decode,
			},
		})
		if mkErr != nil {
			t.Fatalf("%s setup: %v", tc.name, mkErr)
		}
		if err := badRepo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s err = %v, want %v", tc.name, err, tc.wantErr)
		}
		if db.txCount() != txsBefore {
			t.Fatalf("%s opened unexpected write tx", tc.name)
		}
	}

	// BatchProjection error skips single Projections and rolls back tx.
	batchErr := errors.New("batch projection failed")
	singleCalled := false
	batchFailRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Order", Codec: jsonAccountCodec(),
		BatchProjections: []postgres.BatchProjection{
			postgres.BatchProjectionFunc(func(context.Context, pgx.Tx, []store.PersistedEvent) error { return batchErr }),
		},
		Projections: []postgres.Projection{
			postgres.ProjectionFunc(func(context.Context, pgx.Tx, store.PersistedEvent) error {
				singleCalled = true
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("batchFailRepo: %v", err)
	}
	if err := batchFailRepo.Append(context.Background(), "ord-batch", 0, []any{orderAdded{Delta: 1}}); !errors.Is(err, batchErr) {
		t.Fatalf("batch err = %v, want %v", err, batchErr)
	}
	if singleCalled || !db.txs[db.txCount()-1].rolledBack {
		t.Fatalf("singleCalled=%v rolledBack=%v, want false / true", singleCalled, db.txs[db.txCount()-1].rolledBack)
	}

	// Projection failure on 2nd event rolls back write tx.
	projErr := errors.New("proj fail on 2nd")
	calls := 0
	projFailRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Order", Codec: jsonAccountCodec(),
		Projections: []postgres.Projection{
			postgres.ProjectionFunc(func(context.Context, pgx.Tx, store.PersistedEvent) error {
				calls++
				if calls == 2 {
					return projErr
				}
				return nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("projFailRepo: %v", err)
	}
	if err := projFailRepo.Append(context.Background(), "ord-proj2", 0, []any{orderAdded{Delta: 1}, orderAdded{Delta: 2}}); !errors.Is(err, projErr) {
		t.Fatalf("proj fail err = %v, want %v", err, projErr)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected projection failure rollback")
	}

	// Non-OCC EventStore.Append error (duplicate event id) is NOT wrapped with ErrConcurrencyConflict and rolls back tx.
	dupRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: db, EventStore: s, StreamType: "Order", Codec: jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("dupRepo: %v", err)
	}
	s.appendErr = store.ErrDuplicateEventID
	dupErr := dupRepo.Append(context.Background(), "ord-dup", 0, []any{orderAdded{Delta: 1}})
	if !errors.Is(dupErr, store.ErrDuplicateEventID) || postgres.IsConcurrencyConflict(dupErr) {
		t.Fatalf("unexpected non-OCC error wrapping: %v", dupErr)
	}
	if !db.txs[db.txCount()-1].rolledBack {
		t.Fatal("expected non-OCC append error rollback")
	}
	s.appendErr = nil

	// ReadStream error & Decode error roll back read_tx cleanly.
	readErr := errors.New("read fail")
	s.readErr = readErr
	if _, _, err := repo.Load(context.Background(), "ord-1"); !errors.Is(err, readErr) || !db.txs[db.txCount()-1].rolledBack {
		t.Fatalf("ReadStream err=%v rolledBack=%v", err, db.txs[db.txCount()-1].rolledBack)
	}
	s.readErr = nil
}

func TestNarrowVsTxEventRepository_StrictOwnershipAndExplicitTx(t *testing.T) {
	t.Parallel()

	// 1. Narrow EventRepository closes read_tx before decide, opens write_tx after decide,
	//    and errors with ErrTxAlreadyActive if called inside NewTxMiddleware / AdoptTx (even on no-op).
	narrowDB := &spyDB{}
	narrowStore := newSpyStore(narrowDB)
	narrowRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: narrowDB, EventStore: narrowStore, StreamType: "Order", Codec: jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	narrowHandler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(narrowRepo, accountDecider()),
		func(delta int) string { return "ord-n" },
		func(_ accountState, delta int) ([]any, error) {
			narrowDB.recordTimeline("decide")
			return []any{orderAdded{Delta: delta}}, nil
		},
	)

	bus, err := command.NewBus(command.WithMiddleware(command.ObservabilityMiddleware()))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, narrowHandler)
	if dispatchErr := bus.Dispatch(context.Background(), 25); dispatchErr != nil {
		t.Fatalf("Dispatch narrow: %v", dispatchErr)
	}
	wantNarrow := []string{
		"read_tx:begin", "store:read", "read_tx:commit",
		"decide",
		"write_tx:begin", "store:append", "write_tx:commit",
	}
	if !slices.Equal(narrowDB.snapshotTimeline(), wantNarrow) {
		t.Fatalf("narrow timeline = %v, want %v", narrowDB.snapshotTimeline(), wantNarrow)
	}

	// Mixing NewTxMiddleware with narrow EventRepository fails loudly with ErrTxAlreadyActive.
	badMixedBus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(narrowDB)))
	if err != nil {
		t.Fatalf("NewBus mixed: %v", err)
	}
	command.MustRegister(badMixedBus, narrowHandler)
	if mixedErr := badMixedBus.Dispatch(context.Background(), 5); !errors.Is(mixedErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("mixed wide middleware + narrow repo err = %v, want ErrTxAlreadyActive", mixedErr)
	}
	activeCtx, adoptErr := postgres.AdoptTx(context.Background(), &spyTx{db: narrowDB, id: 55})
	if adoptErr != nil {
		t.Fatalf("AdoptTx: %v", adoptErr)
	}
	if appErr := narrowRepo.Append(activeCtx, "ord-n", 1, nil); !errors.Is(appErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("narrowRepo.Append(empty) inside active tx err = %v, want ErrTxAlreadyActive", appErr)
	}

	// 2. TxEventRepository requires active ctx tx (ErrMissingTx outside), works inside MustTxMiddleware
	//    without starting/committing/rolling back its own tx, and verifies Load Decode errors roll back.
	wideDB := &spyDB{}
	wideStore := newSpyStore(wideDB)
	txRepo, err := postgres.NewTxEventRepository(postgres.TxEventRepositoryConfig[any]{
		EventStore: wideStore, StreamType: "Order", Codec: jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("NewTxEventRepository: %v", err)
	}

	if _, _, missErr := txRepo.Load(context.Background(), "ord-w"); !errors.Is(missErr, postgres.ErrMissingTx) {
		t.Fatalf("txRepo.Load(no tx) err = %v, want ErrMissingTx", missErr)
	}
	if missErr := txRepo.Append(context.Background(), "ord-w", 0, nil); !errors.Is(missErr, postgres.ErrMissingTx) {
		t.Fatalf("txRepo.Append(no tx, empty) err = %v, want ErrMissingTx", missErr)
	}

	wideHandler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(txRepo, accountDecider()),
		func(delta int) string { return "ord-w" },
		func(_ accountState, delta int) ([]any, error) {
			wideDB.recordTimeline("decide")
			return []any{orderAdded{Delta: delta}}, nil
		},
	)
	wideBus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(wideDB)))
	if err != nil {
		t.Fatalf("NewBus wide: %v", err)
	}
	command.MustRegister(wideBus, wideHandler)
	if dispatchErr := wideBus.Dispatch(context.Background(), 10); dispatchErr != nil {
		t.Fatalf("Dispatch wide: %v", dispatchErr)
	}
	wantWide := []string{
		"write_tx:begin",
		"store:read",
		"decide",
		"store:append",
		"write_tx:commit",
	}
	if !slices.Equal(wideDB.snapshotTimeline(), wantWide) {
		t.Fatalf("wide timeline = %v, want %v", wideDB.snapshotTimeline(), wantWide)
	}

	// Verify explicit LoadTx / AppendTx guards and round-trip on both repository types.
	var typedNilTx *spyTx
	manualTx := &spyTx{db: wideDB, id: 88}
	for _, fn := range []struct {
		name string
		call func(context.Context, pgx.Tx, string) error
	}{
		{name: "EventRepository.LoadTx", call: func(c context.Context, tx pgx.Tx, id string) error { _, _, e := narrowRepo.LoadTx(c, tx, id); return e }},
		{name: "EventRepository.AppendTx", call: func(c context.Context, tx pgx.Tx, id string) error { return narrowRepo.AppendTx(c, tx, id, 0, nil) }},
		{name: "TxEventRepository.LoadTx", call: func(c context.Context, tx pgx.Tx, id string) error { _, _, e := txRepo.LoadTx(c, tx, id); return e }},
		{name: "TxEventRepository.AppendTx", call: func(c context.Context, tx pgx.Tx, id string) error { return txRepo.AppendTx(c, tx, id, 0, nil) }},
	} {
		if e := fn.call(nil, manualTx, "x"); !errors.Is(e, persistence.ErrNilContext) {
			t.Fatalf("%s(nil ctx) = %v", fn.name, e)
		}
		if e := fn.call(context.Background(), nil, "x"); !errors.Is(e, postgres.ErrNilTx) {
			t.Fatalf("%s(nil tx) = %v", fn.name, e)
		}
		if e := fn.call(context.Background(), typedNilTx, "x"); !errors.Is(e, postgres.ErrNilTx) {
			t.Fatalf("%s(typed-nil tx) = %v", fn.name, e)
		}
		if e := fn.call(context.Background(), manualTx, " "); !errors.Is(e, persistence.ErrEmptyTargetID) {
			t.Fatalf("%s(blank id) = %v", fn.name, e)
		}
	}
	if e := narrowRepo.AppendTx(context.Background(), manualTx, "x", -1, nil); !errors.Is(e, persistence.ErrInvalidVersion) {
		t.Fatalf("AppendTx(-1) = %v", e)
	}
	if e := txRepo.AppendTx(context.Background(), manualTx, "ord-manual", 0, []any{orderAdded{Delta: 77}}); e != nil {
		t.Fatalf("txRepo.AppendTx: %v", e)
	}
	evs, ver, err := txRepo.LoadTx(context.Background(), manualTx, "ord-w")
	if err != nil || ver != 1 || len(evs) != 1 || evs[0] != (orderAdded{Delta: 10}) {
		t.Fatalf("txRepo.LoadTx = (%v, %d, %v), want [Delta:10] ver=1", evs, ver, err)
	}
	if e := narrowRepo.AppendTx(context.Background(), manualTx, "ord-n", 1, []any{orderAdded{Delta: 5}}); e != nil {
		t.Fatalf("narrowRepo.AppendTx: %v", e)
	}
	nEvs, nVer, err := narrowRepo.LoadTx(context.Background(), manualTx, "ord-n")
	if err != nil || nVer != 2 || !slices.Equal(nEvs, []any{orderAdded{Delta: 25}, orderAdded{Delta: 5}}) {
		t.Fatalf("narrowRepo.LoadTx = (%v, %d, %v), want [25, 5] ver=2", nEvs, nVer, err)
	}

	// Verify Load Decode error rolls back read_tx cleanly.
	decodeFail := errors.New("corrupt json")
	badDecRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: wideDB, EventStore: wideStore, StreamType: "Order",
		Codec: postgres.FuncEventCodec[any]{
			EncodeFunc: jsonAccountCodec().Encode,
			DecodeFunc: func(context.Context, store.PersistedEvent) (any, error) { return nil, decodeFail },
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository badDec: %v", err)
	}
	if _, _, loadErr := badDecRepo.Load(context.Background(), "ord-w"); !errors.Is(loadErr, decodeFail) || !wideDB.txs[wideDB.txCount()-1].rolledBack {
		t.Fatalf("Load decode error=%v rolledBack=%v", loadErr, wideDB.txs[wideDB.txCount()-1].rolledBack)
	}
}

type spyProjection struct{}

func (*spyProjection) Handle(context.Context, pgx.Tx, store.PersistedEvent) error { return nil }

type spyBatchProjection struct{}

func (*spyBatchProjection) Handle(context.Context, pgx.Tx, []store.PersistedEvent) error {
	return nil
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

func TestStateRepository_InReadTxAndInTxNarrowStrictness(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	repo := &sqlStateRepo{db: db, records: make(map[string]accountState)}
	h := persistence.MustNewCommandHandler(
		persistence.FromStateRepository(repo, accountDecider()),
		func(delta int) string { return "st-1" },
		func(_ accountState, delta int) ([]any, error) {
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

	// Mixing NewTxMiddleware with a narrow InReadTx/InTx StateRepository fails loudly with ErrTxAlreadyActive.
	bus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(db)))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, h)
	if mixedErr := bus.Dispatch(context.Background(), 5); !errors.Is(mixedErr, postgres.ErrTxAlreadyActive) {
		t.Fatalf("state repo inside TxMiddleware err = %v, want ErrTxAlreadyActive", mixedErr)
	}
}
