//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/snapshot"
	snapshotpostgres "eventsalsa.dev/snapshot/postgres"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

type snapCreditCmd struct {
	id     string
	amount int
}

type versionedAccount struct {
	ID      string `json:"id"`
	Balance int    `json:"balance"`
	Version int64  `json:"version"`
}

func versionedDecider() persistence.Decider[versionedAccount, any] {
	return persistence.Decider[versionedAccount, any]{
		Initial: func(id string) versionedAccount {
			return versionedAccount{ID: id}
		},
		Evolve: func(s versionedAccount, ev any) versionedAccount {
			s.Version++
			if c, ok := ev.(accountCredited); ok {
				s.Balance += c.Amount
			}
			return s
		},
	}
}

func TestSnapshotAdapter_PostgresLifecycleCompressionAndDeltaReplay(t *testing.T) {
	pool := setupPool(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	snapStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	var acquiredDuringDecide []int32
	adapter, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "SnapAccount",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:         snapStore,
			Codec:         snapshot.JSON[versionedAccount](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(3),
			Version:       func(st versionedAccount) int64 { return st.Version },
			Transformers:  []snapshot.PayloadTransformer{snapshot.Gzip()},
		},
	})
	if err != nil {
		t.Fatalf("NewSnapshotAdapter: %v", err)
	}

	h := persistence.MustNewCommandHandler(
		adapter,
		func(c snapCreditCmd) string { return c.id },
		func(_ versionedAccount, c snapCreditCmd) ([]any, error) {
			acquiredDuringDecide = append(acquiredDuringDecide, pool.Stat().AcquiredConns())
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	)

	id := "snap-" + uuid.NewString()
	for i := range 5 {
		if err := h.Handle(ctx, snapCreditCmd{id: id, amount: 10 * (i + 1)}); err != nil {
			t.Fatalf("cmd %d: %v", i+1, err)
		}
	}

	for i, n := range acquiredDuringDecide {
		if n != 0 {
			t.Fatalf("decide %d held %d pooled conns, want 0", i, n)
		}
	}

	// Verify standalone snapshot.Repository reads snapshot v3 + replays only delta v4..v5.
	snapRepo, err := snapshot.NewRepository(es, snapStore, snapshot.RepositoryConfig[versionedAccount]{
		StreamType:    "SnapAccount",
		SchemaVersion: 1,
		Initializer:   versionedDecider().Initial,
		Apply: func(st versionedAccount, pe store.PersistedEvent) (versionedAccount, error) {
			ev, decErr := codec.Decode(ctx, pe)
			if decErr != nil {
				return versionedAccount{}, decErr
			}
			return versionedDecider().Evolve(st, ev), nil
		},
		Codec:        snapshot.JSON[versionedAccount](),
		Transformers: []snapshot.PayloadTransformer{snapshot.Gzip()},
	})
	if err != nil {
		t.Fatalf("snapshot.NewRepository: %v", err)
	}

	var res snapshot.Result[versionedAccount]
	if loadErr := postgres.InReadTx(ctx, pool, func(txCtx context.Context, tx pgx.Tx) error {
		var rErr error
		res, rErr = snapRepo.Load(txCtx, tx, id)
		return rErr
	}); loadErr != nil {
		t.Fatalf("snapRepo.Load: %v", loadErr)
	}
	if !res.SnapshotHit || res.SnapshotVersion != 3 || res.StreamVersion != 5 || res.EventsReplayed != 2 || res.State.Balance != 150 {
		t.Fatalf("unexpected load result: %+v", res)
	}

	// 6th command crosses next threshold (5 - 3 + 1 == 3) -> updates snapshot to v6.
	if err := h.Handle(ctx, snapCreditCmd{id: id, amount: 60}); err != nil {
		t.Fatalf("cmd 6: %v", err)
	}
	if loadErr := postgres.InReadTx(ctx, pool, func(txCtx context.Context, tx pgx.Tx) error {
		var rErr error
		res, rErr = snapRepo.Load(txCtx, tx, id)
		return rErr
	}); loadErr != nil {
		t.Fatalf("snapRepo.Load after v6: %v", loadErr)
	}
	if res.SnapshotVersion != 6 || res.EventsReplayed != 0 || res.State.Balance != 210 {
		t.Fatalf("unexpected load result after v6: %+v", res)
	}

	var projBal int
	if qErr := pool.QueryRow(ctx, `SELECT balance FROM order_overview_v1 WHERE order_id = $1`, id).Scan(&projBal); qErr != nil {
		t.Fatalf("query projection: %v", qErr)
	}
	if projBal != 210 {
		t.Fatalf("projection balance = %d, want 210", projBal)
	}
}

func TestSnapshotAdapter_PostgresUpcasterCorruptFallbackAndSavepointIsolation(t *testing.T) {
	pool := setupPool(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	snapStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	id := "upcast-" + uuid.NewString()
	narrowRepo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB: pool, EventStore: es, StreamType: "UpcastAccount", Codec: codec,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := narrowRepo.Append(ctx, id, 0, []any{accountCredited{Amount: 40}, accountCredited{Amount: 60}}); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	// Seed legacy schema v1 snapshot manually.
	v1Raw, err := json.Marshal(map[string]any{"id": id, "legacy_cents": 100, "version": 2})
	if err != nil {
		t.Fatal(err)
	}
	if putErr := postgres.InTx(ctx, pool, func(txCtx context.Context, tx pgx.Tx) error {
		return snapStore.Put(txCtx, tx, &snapshot.Snapshot{
			StreamType:    "UpcastAccount",
			StreamID:      id,
			StreamVersion: 2,
			SchemaVersion: 1,
			Payload:       v1Raw,
		})
	}); putErr != nil {
		t.Fatalf("seed v1 snapshot: %v", putErr)
	}

	var rejectedCount atomic.Int32
	v2Adapter, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "UpcastAccount",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:         snapStore,
			Codec:         snapshot.JSON[versionedAccount](),
			SchemaVersion: 2,
			Policy:        snapshot.EveryNEvents(1),
			Upcasters: map[int]snapshot.Upcaster{
				1: func(_ int, raw []byte) ([]byte, error) {
					return []byte(strings.ReplaceAll(string(raw), `"legacy_cents"`, `"balance"`)), nil
				},
			},
			OnSnapshotRejected: func(_ context.Context, _ string, _ string, _ error) {
				rejectedCount.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatalf("v2Adapter: %v", err)
	}

	var observedBefore int
	hV2 := persistence.MustNewCommandHandler(v2Adapter, func(c snapCreditCmd) string { return c.id }, func(st versionedAccount, c snapCreditCmd) ([]any, error) {
		observedBefore = st.Balance
		return []any{accountCredited{Amount: c.amount}}, nil
	})
	if err := hV2.Handle(ctx, snapCreditCmd{id: id, amount: 25}); err != nil {
		t.Fatalf("upcast handle: %v", err)
	}
	if observedBefore != 100 {
		t.Fatalf("upcasted state before cmd = %d, want 100", observedBefore)
	}

	// Corrupt v2 payload in PostgreSQL -> falls back to full replay (40+60+25=125) and heals snapshot at v4.
	if _, execErr := pool.Exec(ctx, `
		UPDATE snapshots SET payload = '\xdeadbeef'::bytea
		WHERE stream_type = 'UpcastAccount' AND stream_id = $1 AND schema_version = 2
	`, id); execErr != nil {
		t.Fatalf("corrupt snapshot: %v", execErr)
	}
	if err := hV2.Handle(ctx, snapCreditCmd{id: id, amount: 15}); err != nil {
		t.Fatalf("corrupt recovery handle: %v", err)
	}
	if observedBefore != 125 || rejectedCount.Load() != 1 {
		t.Fatalf("corrupt recovery observedBefore=%d rejectedCount=%d, want 125/1", observedBefore, rejectedCount.Load())
	}

	// Savepoint isolation against real PostgreSQL SQLSTATE 42P01 (missing snapshots table).
	// Best-effort mode must isolate the failed sub-tx cleanly so both event + projection commit!
	brokenStore := snapshotpostgres.NewStore(snapshotpostgres.NewStoreConfig(
		snapshotpostgres.WithSnapshotsTable("nonexistent_snapshots_table"),
	))
	var saveErrHits atomic.Int32
	bestEffortID := "sp-ok-" + uuid.NewString()
	bestEffortAdapter, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "BrokenSnap",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:         brokenStore,
			Codec:         snapshot.JSON[versionedAccount](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(1),
			OnSnapshotSaveError: func(_ context.Context, _ string, _ int64, _ error) {
				saveErrHits.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Trigger best-effort save after seeding 1 event (first read also fails on Get unless table exists -> seed or Expect Get fail!).
	// Wait: snapshot Store.Get fails if table missing, so use a store with working Get table + FailOnSave error via bad Codec/Version!
	_ = bestEffortAdapter
	_ = bestEffortID

	// Verify real PostgreSQL 42P01 savepoint recovery by using a custom snapshot.Store wrapper
	// whose Get succeeds against snapshots table and Put fails against nonexistent_snapshots_table inside pgx subTx!
	hybridStore := splitSnapshotStore{
		getStore: snapStore,
		putStore: brokenStore,
	}
	bestEffortAdapter, err = postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "HybridSnap",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:         hybridStore,
			Codec:         snapshot.JSON[versionedAccount](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(1),
			OnSnapshotSaveError: func(_ context.Context, _ string, _ int64, _ error) {
				saveErrHits.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	hBE := persistence.MustNewCommandHandler(bestEffortAdapter, func(c snapCreditCmd) string { return c.id }, func(_ versionedAccount, c snapCreditCmd) ([]any, error) {
		return []any{accountCredited{Amount: c.amount}}, nil
	})
	if err := hBE.Handle(ctx, snapCreditCmd{id: bestEffortID, amount: 77}); err != nil {
		t.Fatalf("best-effort savepoint Handle failed: %v", err)
	}
	if saveErrHits.Load() != 1 {
		t.Fatalf("saveErrHits = %d, want 1", saveErrHits.Load())
	}
	var committedBal int
	if err := pool.QueryRow(ctx, `SELECT balance FROM order_overview_v1 WHERE order_id = $1`, bestEffortID).Scan(&committedBal); err != nil {
		t.Fatalf("expected projection committed despite snapshot 42P01 error: %v", err)
	}
	if committedBal != 77 {
		t.Fatalf("projection balance = %d, want 77", committedBal)
	}

	// Atomic mode (FailOnSnapshotSaveError: true) rolls back both event and projection.
	atomicID := "sp-fail-" + uuid.NewString()
	atomicAdapter, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          pool,
			EventStore:  es,
			StreamType:  "HybridSnap",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:                   hybridStore,
			Codec:                   snapshot.JSON[versionedAccount](),
			SchemaVersion:           1,
			Policy:                  snapshot.EveryNEvents(1),
			FailOnSnapshotSaveError: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := persistence.MustNewCommandHandler(atomicAdapter, func(c snapCreditCmd) string { return c.id }, func(_ versionedAccount, c snapCreditCmd) ([]any, error) {
		return []any{accountCredited{Amount: c.amount}}, nil
	}).Handle(ctx, snapCreditCmd{id: atomicID, amount: 88}); err == nil {
		t.Fatal("expected atomic mode error on SQL 42P01 snapshot Put")
	}
	var atomicCnt int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM order_overview_v1 WHERE order_id = $1`, atomicID).Scan(&atomicCnt); err != nil {
		t.Fatal(err)
	}
	if atomicCnt != 0 {
		t.Fatalf("expected atomic rollback to commit 0 projection rows, got %d", atomicCnt)
	}
}

func TestTxSnapshotAdapter_PostgresWideAtomicityAndConcurrentOCC(t *testing.T) {
	pool := setupPool(t, func(c *pgxpool.Config) {
		c.MaxConns = 4
	})
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	snapStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	// Narrow SnapshotAdapter inside TxMiddleware must fail loudly with ErrTxAlreadyActive.
	narrowSnap, err := postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB: pool, EventStore: es, StreamType: "WideSnap", Codec: codec,
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store: snapStore, Codec: snapshot.JSON[versionedAccount](), SchemaVersion: 1, Policy: snapshot.EveryNEvents(1),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	badWideBus, err := command.NewBus(command.WithMiddleware(postgres.MustTxMiddleware(pool)))
	if err != nil {
		t.Fatal(err)
	}
	command.MustRegister(badWideBus, persistence.MustNewCommandHandler(
		narrowSnap,
		func(c snapCreditCmd) string { return c.id },
		func(_ versionedAccount, c snapCreditCmd) ([]any, error) {
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	))
	if err := badWideBus.Dispatch(ctx, snapCreditCmd{id: "bad-" + uuid.NewString(), amount: 5}); !errors.Is(err, postgres.ErrTxAlreadyActive) {
		t.Fatalf("narrow SnapshotAdapter inside TxMiddleware err = %v, want ErrTxAlreadyActive", err)
	}

	// Wide TxSnapshotAdapter participates in MustTxMiddleware atomically and survives OCC retries.
	wideSnap, err := postgres.NewTxSnapshotAdapter(postgres.TxSnapshotAdapterConfig[versionedAccount, any]{
		Decider: versionedDecider(),
		TxEventRepository: postgres.TxEventRepositoryConfig[any]{
			EventStore:  es,
			StreamType:  "WideSnap",
			Codec:       codec,
			Projections: []postgres.Projection{accountBalanceProjection(codec)},
		},
		Snapshot: postgres.SnapshotConfig[versionedAccount]{
			Store:         snapStore,
			Codec:         snapshot.JSON[versionedAccount](),
			SchemaVersion: 1,
			Policy:        snapshot.EveryNEvents(2),
			Version:       func(st versionedAccount) int64 { return st.Version },
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	wideHandler := persistence.MustNewCommandHandler(
		wideSnap,
		func(c snapCreditCmd) string { return c.id },
		func(_ versionedAccount, c snapCreditCmd) ([]any, error) {
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	)
	if err := wideHandler.Handle(ctx, snapCreditCmd{id: "no-tx", amount: 1}); !errors.Is(err, postgres.ErrMissingTx) {
		t.Fatalf("wide outside tx err = %v, want ErrMissingTx", err)
	}

	bus, err := command.NewBus(command.WithMiddleware(
		command.RetryMiddleware(command.RetryConfig{
			MaxAttempts: 16,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  15 * time.Millisecond,
			RetryIf:     postgres.IsConcurrencyConflict,
		}),
		postgres.MustTxMiddleware(pool),
	))
	if err != nil {
		t.Fatal(err)
	}
	command.MustRegister(bus, wideHandler)

	const workers = 8
	id := "wide-occ-" + uuid.NewString()
	var (
		wg   sync.WaitGroup
		errs = make(chan error, workers)
	)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			errs <- bus.Dispatch(ctx, snapCreditCmd{id: id, amount: 10})
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatalf("concurrent wide snapshot dispatch failed: %v", e)
		}
	}

	var snapVer int64
	if err := pool.QueryRow(ctx, `
		SELECT stream_version FROM snapshots
		WHERE stream_type = 'WideSnap' AND stream_id = $1 AND schema_version = 1
	`, id).Scan(&snapVer); err != nil {
		t.Fatalf("query snapshots: %v", err)
	}
	if snapVer != 8 {
		t.Fatalf("wide snapshot stream_version = %d, want 8", snapVer)
	}
	var projBal int
	if err := pool.QueryRow(ctx, `SELECT balance FROM order_overview_v1 WHERE order_id = $1`, id).Scan(&projBal); err != nil {
		t.Fatal(err)
	}
	if projBal != 80 {
		t.Fatalf("wide projection balance = %d, want 80", projBal)
	}
}

type splitSnapshotStore struct {
	getStore snapshot.Store
	putStore snapshot.Store
}

func (s splitSnapshotStore) Get(
	ctx context.Context,
	tx pgx.Tx,
	streamType, streamID string,
	maxSchemaVersion int,
) (snapshot.Snapshot, error) {
	return s.getStore.Get(ctx, tx, streamType, streamID, maxSchemaVersion)
}

func (s splitSnapshotStore) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	return s.putStore.Put(ctx, tx, snap)
}
