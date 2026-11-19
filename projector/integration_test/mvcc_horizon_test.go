//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"

	projectorpkg "eventsalsa.dev/projector"
)

// splitPauseTxProjection writes handled events to test_projection_events and
// blocks once on pauseAtPos until released or ctx is canceled, letting tests
// split a single multi-event transaction across batch boundaries deterministically.
type splitPauseTxProjection struct {
	name       string
	label      string
	pauseAtPos int64
	paused     chan struct{}
	release    chan struct{}
	pauseOnce  sync.Once
}

func (p *splitPauseTxProjection) Name() string { return p.name }

//nolint:gocritic // hugeParam: implements the Projection contract
func (p *splitPauseTxProjection) Handle(ctx context.Context, tx pgx.Tx, event store.PersistedEvent) error {
	if event.GlobalPosition == p.pauseAtPos {
		var triggered bool
		p.pauseOnce.Do(func() {
			triggered = true
			close(p.paused)
		})
		if triggered {
			select {
			case <-p.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	_, err := tx.Exec(ctx, `
INSERT INTO test_projection_events (
	projection_name,
	global_position,
	stream_type,
	stream_id,
	event_type,
	handled_by,
	attempt_no
) VALUES ($1, $2, $3, $4, $5, $6, $7)
`, p.name, event.GlobalPosition, event.StreamType, event.StreamID, event.EventType, p.label, 1)
	if err != nil {
		return fmt.Errorf("insert split-pause projection row: %w", err)
	}

	return nil
}

func makeOrderEvent(streamID, eventType string, num int) store.Event {
	return store.Event{
		StreamType:   "Order",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    eventType,
		EventVersion: 1,
		Payload:      []byte(fmt.Sprintf(`{"event_number":%d}`, num)),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now().UTC(),
	}
}

func TestMVCC_ConcurrentOpenWriteAndRollback_AllShapes(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	txEvent := newTestProjection("mvcc-tx-event", "projector-1", nil)
	txBatch := &batchTestProjection{name: "mvcc-tx-batch", instanceLabel: "projector-1"}
	detEvent := &scriptedDetachedProjection{name: "mvcc-det-event", entered: make(chan struct{}, 64)}
	detBatch := &scriptedDetachedBatchProjection{name: "mvcc-det-batch"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(txEvent); err != nil {
		t.Fatalf("register tx event projection: %v", err)
	}
	if err := registry.AddBatch(txBatch); err != nil {
		t.Fatalf("register tx batch projection: %v", err)
	}
	if err := registry.AddDetached(detEvent); err != nil {
		t.Fatalf("register detached event projection: %v", err)
	}
	if err := registry.AddDetachedBatch(detBatch); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithPollInterval(50*time.Millisecond),
		projectorpkg.WithMaxPollInterval(50*time.Millisecond),
		projectorpkg.WithDispatcherInterval(25*time.Millisecond),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 4 {
			return fmt.Errorf("assignments=%d, want 4", len(assignments))
		}
		for _, a := range assignments {
			if !a.Assigned {
				return fmt.Errorf("projection %s not assigned yet", a.ProjectionName)
			}
		}
		return nil
	})

	// Open a lower-XID transaction and hold it open before later transactions commit.
	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
	rolledBackPos := held.events[0].GlobalPosition
	committed := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	latest := cursorOf(committed)

	time.Sleep(400 * time.Millisecond)

	for _, name := range []string{txEvent.Name(), txBatch.Name(), detEvent.Name(), detBatch.Name()} {
		if checkpoint := getCheckpoint(t, controlDB, name); !checkpoint.IsZero() {
			t.Fatalf("%s checkpoint=%+v want zero while open write pins xmin", name, checkpoint)
		}
	}
	if rows := getHandledRows(t, controlDB, txEvent.Name()); len(rows) != 0 {
		t.Fatalf("tx-event handled %d rows while xmin pinned, want 0", len(rows))
	}
	if calls := txBatch.Calls(); calls != 0 {
		t.Fatalf("tx-batch calls=%d while xmin pinned, want 0", calls)
	}
	if applied := detEvent.uniqueApplied(); applied != 0 {
		t.Fatalf("det-event applied=%d while xmin pinned, want 0", applied)
	}
	if calls := detBatch.callsCount(); calls != 0 {
		t.Fatalf("det-batch calls=%d while xmin pinned, want 0", calls)
	}

	held.Rollback(t)
	unblockedAt := time.Now()

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{txEvent.Name(), txBatch.Name(), detEvent.Name(), detBatch.Name()} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%+v want %+v", name, checkpoint, latest)
			}
		}
		if rows := getHandledRows(t, controlDB, txEvent.Name()); len(rows) != 2 {
			return fmt.Errorf("tx-event rows=%d, want 2", len(rows))
		}
		if rows := getHandledRows(t, controlDB, txBatch.Name()); len(rows) != 2 {
			return fmt.Errorf("tx-batch rows=%d, want 2", len(rows))
		}
		if applied := detEvent.uniqueApplied(); applied != 2 {
			return fmt.Errorf("det-event applied=%d, want 2", applied)
		}
		if applied := detBatch.timesApplied(committed[1].GlobalPosition); applied != 1 {
			return fmt.Errorf("det-batch applied last event %d times, want 1", applied)
		}
		return nil
	})

	if elapsed := time.Since(unblockedAt); elapsed >= 1500*time.Millisecond {
		t.Fatalf("unblocking took %v after rollback, expected < 1.5s", elapsed)
	}
	if times := detEvent.timesApplied(rolledBackPos); times != 0 {
		t.Fatalf("rolled-back event %d applied %d times by det-event", rolledBackPos, times)
	}
	if times := detBatch.timesApplied(rolledBackPos); times != 0 {
		t.Fatalf("rolled-back event %d applied %d times by det-batch", rolledBackPos, times)
	}

	harness.stop(t)
}

func TestMVCC_InvertedSequenceAndXID_BothDeliveredExactlyOnce(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	// Force txA to receive an earlier xid8 BEFORE txB claims a lower global_position.
	txA, err := controlDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txA: %v", err)
	}
	defer func() { _ = txA.Rollback(ctx) }()

	var xidA string
	if err := txA.QueryRow(ctx, `SELECT pg_current_xact_id()::text`).Scan(&xidA); err != nil {
		t.Fatalf("assign xid for txA: %v", err)
	}

	txB, err := controlDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin txB: %v", err)
	}
	defer func() { _ = txB.Rollback(ctx) }()

	resB, err := eventStore.Append(ctx, txB, store.NoStream(), []store.Event{
		makeOrderEvent(uuid.NewString(), "Order.inverted.B", 1),
	})
	if err != nil {
		t.Fatalf("append txB: %v", err)
	}
	if err := txB.Commit(ctx); err != nil {
		t.Fatalf("commit txB: %v", err)
	}

	resA, err := eventStore.Append(ctx, txA, store.NoStream(), []store.Event{
		makeOrderEvent(uuid.NewString(), "Order.inverted.A", 2),
	})
	if err != nil {
		t.Fatalf("append txA: %v", err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatalf("commit txA: %v", err)
	}

	eventA := resA.Events[0]
	eventB := resB.Events[0]
	if eventA.TransactionID >= eventB.TransactionID {
		t.Fatalf("expected xidA (%d) < xidB (%d)", eventA.TransactionID, eventB.TransactionID)
	}
	if eventA.GlobalPosition <= eventB.GlobalPosition {
		t.Fatalf("expected posA (%d) > posB (%d)", eventA.GlobalPosition, eventB.GlobalPosition)
	}

	projection := newTestProjection("inverted-xid-pos", "projector-1", nil)
	options := append(defaultProjectorOptions(), projectorpkg.WithBatchSize(1))
	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, options...)

	wantFinal := cursorOfEvent(eventB)
	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != wantFinal {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, wantFinal)
		}
		return nil
	})
	harness.stop(t)

	positions := getHandledPositionsInOrder(t, controlDB, projection.Name())
	if len(positions) != 2 {
		t.Fatalf("handled rows=%d, want 2 (positions=%v)", len(positions), positions)
	}
	if positions[0] != eventA.GlobalPosition || positions[1] != eventB.GlobalPosition {
		t.Fatalf("delivery positions = %v, want commit-horizon order [%d, %d]",
			positions, eventA.GlobalPosition, eventB.GlobalPosition)
	}
}

func TestMVCC_MidTransactionBatchSplitAndRestartRecovery(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	appended := appendTestEvents(t, controlDB, eventStore, 5, "Order")
	for i := 1; i < len(appended); i++ {
		if appended[i].TransactionID != appended[0].TransactionID {
			t.Fatalf("expected single transaction ID across batch, got %d != %d", appended[i].TransactionID, appended[0].TransactionID)
		}
	}

	const projName = "split-tx-recovery"
	pauseProj := &splitPauseTxProjection{
		name:       projName,
		label:      "projector-1",
		pauseAtPos: appended[2].GlobalPosition,
		paused:     make(chan struct{}),
		release:    make(chan struct{}),
	}
	defer close(pauseProj.release)

	registry1 := projectorpkg.NewRegistry()
	if err := registry1.AddProjection(pauseProj); err != nil {
		t.Fatalf("register split projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithBatchSize(2),
		projectorpkg.WithShutdownTimeout(time.Second),
	)
	harness1 := startTestProjectorFromRegistry(t, "projector-1", registry1, 8, options...)

	select {
	case <-pauseProj.paused:
	case <-time.After(defaultWaitTimeout):
		t.Fatal("timeout waiting for second batch to start on third event")
	}

	midCursor := cursorOfEvent(appended[1])
	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projName); checkpoint != midCursor {
			return fmt.Errorf("mid-tx checkpoint=%+v want %+v", checkpoint, midCursor)
		}
		return nil
	})

	harness1.stop(t)

	if rows := getHandledRows(t, controlDB, projName); len(rows) != 2 {
		t.Fatalf("after interrupted batch 2 handled rows=%d, want 2", len(rows))
	}
	if checkpoint := getCheckpoint(t, controlDB, projName); checkpoint != midCursor {
		t.Fatalf("after stop checkpoint=%+v want %+v", checkpoint, midCursor)
	}

	obs2 := &testIntegrationObserver{}
	resumedProj := newTestProjection(projName, "projector-2", nil)
	opts2 := append(append([]projectorpkg.Option(nil), options...), projectorpkg.WithObserver(obs2))
	harness2 := startTestProjector(t, "projector-2", []*testProjection{resumedProj}, opts2...)
	finalCursor := cursorOfEvent(appended[4])

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projName); checkpoint != finalCursor {
			return fmt.Errorf("post-restart checkpoint=%+v want %+v (assignments=%+v batches=%+v)",
				checkpoint, finalCursor, getAssignments(t, controlDB), obs2.Batches())
		}
		return nil
	})
	harness2.stop(t)

	rows := getHandledRows(t, controlDB, projName)
	if len(rows) != len(appended) {
		t.Fatalf("total handled rows=%d, want %d", len(rows), len(appended))
	}
	for i, want := range appended {
		if rows[i].GlobalPosition != want.GlobalPosition {
			t.Fatalf("row %d position=%d, want %d", i, rows[i].GlobalPosition, want.GlobalPosition)
		}
	}
}

func TestMVCC_SavepointRollbackAndNestedTx(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	tx, err := controlDB.Begin(ctx)
	if err != nil {
		t.Fatalf("begin savepoint tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	r1, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
		makeOrderEvent(uuid.NewString(), "Order.placed", 1),
	})
	if err != nil {
		t.Fatalf("append r1: %v", err)
	}

	if _, err := tx.Exec(ctx, "SAVEPOINT mvcc_sp"); err != nil {
		t.Fatalf("create savepoint: %v", err)
	}

	r2, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
		makeOrderEvent(uuid.NewString(), "Order.rolled_back", 2),
	})
	if err != nil {
		t.Fatalf("append r2: %v", err)
	}

	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT mvcc_sp"); err != nil {
		t.Fatalf("rollback savepoint: %v", err)
	}

	r3, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
		makeOrderEvent(uuid.NewString(), "Order.paid", 3),
	})
	if err != nil {
		t.Fatalf("append r3: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit savepoint tx: %v", err)
	}

	r4 := appendTestEvents(t, controlDB, eventStore, 1, "Order")

	projection := newTestProjection("savepoint-projection", "projector-1", nil)
	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	wantFinal := cursorOfEvent(r4[0])
	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != wantFinal {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, wantFinal)
		}
		return nil
	})
	harness.stop(t)

	rows := getHandledRows(t, controlDB, projection.Name())
	if len(rows) != 3 {
		t.Fatalf("handled rows=%d, want 3 (rolled-back savepoint position %d must not appear)", len(rows), r2.Events[0].GlobalPosition)
	}
	wantPos := []int64{r1.Events[0].GlobalPosition, r3.Events[0].GlobalPosition, r4[0].GlobalPosition}
	for i, pos := range wantPos {
		if rows[i].GlobalPosition != pos {
			t.Fatalf("row %d global_position=%d, want %d", i, rows[i].GlobalPosition, pos)
		}
	}
}

func TestMVCC_HighConcurrencyChaos_TwoDaemonsRebalance(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	const (
		txEvent1Name  = "chaos-tx-event-1"
		txEvent2Name  = "chaos-tx-event-2"
		txBatchName   = "chaos-tx-batch"
		detEvent1Name = "chaos-det-event-1"
		detEvent2Name = "chaos-det-event-2"
		detBatchName  = "chaos-det-batch"
	)

	makeRegistry := func(label string, det1, det2 *scriptedDetachedProjection, detB *scriptedDetachedBatchProjection) *projectorpkg.Registry {
		r := projectorpkg.NewRegistry()
		for _, p := range []*testProjection{
			newTestProjection(txEvent1Name, label, nil),
			newTestProjection(txEvent2Name, label, nil),
		} {
			if err := r.AddProjection(p); err != nil {
				t.Fatalf("register %s: %v", p.Name(), err)
			}
		}
		if err := r.AddBatch(&batchTestProjection{name: txBatchName, instanceLabel: label}); err != nil {
			t.Fatalf("register %s: %v", txBatchName, err)
		}
		if err := r.AddDetached(det1); err != nil {
			t.Fatalf("register %s: %v", det1.Name(), err)
		}
		if err := r.AddDetached(det2); err != nil {
			t.Fatalf("register %s: %v", det2.Name(), err)
		}
		if err := r.AddDetachedBatch(detB); err != nil {
			t.Fatalf("register %s: %v", detB.Name(), err)
		}
		return r
	}

	det1A := &scriptedDetachedProjection{name: detEvent1Name, entered: make(chan struct{}, 256)}
	det2A := &scriptedDetachedProjection{name: detEvent2Name, entered: make(chan struct{}, 256)}
	detBA := &scriptedDetachedBatchProjection{name: detBatchName}

	det1B := &scriptedDetachedProjection{name: detEvent1Name, entered: make(chan struct{}, 256)}
	det2B := &scriptedDetachedProjection{name: detEvent2Name, entered: make(chan struct{}, 256)}
	detBB := &scriptedDetachedBatchProjection{name: detBatchName}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease),
		projectorpkg.WithBatchSize(7),
	)
	daemon1 := startTestProjectorFromRegistry(t, "chaos-1", makeRegistry("chaos-1", det1A, det2A, detBA), 8, options...)
	daemon2 := startTestProjectorFromRegistry(t, "chaos-2", makeRegistry("chaos-2", det1B, det2B, detBB), 8, options...)

	ctx := context.Background()
	contendedStreamID := uuid.NewString()

	var (
		mu             sync.Mutex
		committed      []store.PersistedEvent
		rolledBackPos  []int64
		stopWriters    atomic.Bool
		writersWG      sync.WaitGroup
		writeSeqAtomic atomic.Int64
	)

	const numWriters = 6
	for w := range numWriters {
		writersWG.Add(1)
		go func(workerID int) {
			defer writersWG.Done()

			for !stopWriters.Load() {
				step := writeSeqAtomic.Add(1)
				tx, err := controlDB.Begin(ctx)
				if err != nil {
					return
				}

				switch step % 4 {
				case 0:
					// Savepoint partial rollback plus committed event.
					e1, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
						makeOrderEvent(uuid.NewString(), "Order.chaos", workerID),
					})
					if err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					if _, err := tx.Exec(ctx, "SAVEPOINT sp_chaos"); err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					eDrop, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
						makeOrderEvent(uuid.NewString(), "Order.chaos.drop", workerID),
					})
					if err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp_chaos"); err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					e3, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
						makeOrderEvent(uuid.NewString(), "Order.chaos", workerID),
					})
					if err != nil || tx.Commit(ctx) != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					mu.Lock()
					committed = append(committed, e1.Events...)
					committed = append(committed, e3.Events...)
					for _, e := range eDrop.Events {
						rolledBackPos = append(rolledBackPos, e.GlobalPosition)
					}
					mu.Unlock()
				case 1:
					// Intentional full transaction rollback.
					eDrop, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
						makeOrderEvent(uuid.NewString(), "Order.chaos.abort", workerID),
					})
					_ = tx.Rollback(ctx)
					if err == nil {
						mu.Lock()
						for _, e := range eDrop.Events {
							rolledBackPos = append(rolledBackPos, e.GlobalPosition)
						}
						mu.Unlock()
					}
				case 2:
					// Contended stream (triggers real OCC conflicts across workers).
					stream, err := eventStore.ReadStream(ctx, tx, "Order", contendedStreamID, nil, nil)
					if err != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					res, err := eventStore.Append(ctx, tx, store.Exact(stream.Version()), []store.Event{
						makeOrderEvent(contendedStreamID, "Order.contended", workerID),
					})
					if err != nil || tx.Commit(ctx) != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					mu.Lock()
					committed = append(committed, res.Events...)
					mu.Unlock()
				default:
					// Multi-event commit in one transaction.
					sid := uuid.NewString()
					res, err := eventStore.Append(ctx, tx, store.NoStream(), []store.Event{
						makeOrderEvent(sid, "Order.multi.1", workerID),
						makeOrderEvent(sid, "Order.multi.2", workerID),
					})
					if err != nil || tx.Commit(ctx) != nil {
						_ = tx.Rollback(ctx)
						continue
					}
					mu.Lock()
					committed = append(committed, res.Events...)
					mu.Unlock()
				}
			}
		}(w)
	}

	time.Sleep(600 * time.Millisecond)
	daemon1.stop(t)
	time.Sleep(600 * time.Millisecond)

	stopWriters.Store(true)
	writersWG.Wait()

	mu.Lock()
	expectedEvents := append([]store.PersistedEvent(nil), committed...)
	abortedPositions := append([]int64(nil), rolledBackPos...)
	mu.Unlock()

	if len(expectedEvents) == 0 {
		t.Fatal("expected at least one committed event under chaos test")
	}

	sort.Slice(expectedEvents, func(i, j int) bool {
		return expectedEvents[i].GlobalPosition < expectedEvents[j].GlobalPosition
	})
	head := latestHead(t, controlDB, eventStore)

	projectionNames := []string{txEvent1Name, txEvent2Name, txBatchName, detEvent1Name, detEvent2Name, detBatchName}
	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range projectionNames {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != head {
				return fmt.Errorf("%s checkpoint=%+v want %+v", name, checkpoint, head)
			}
		}
		for _, name := range []string{txEvent1Name, txEvent2Name, txBatchName} {
			if rows := getHandledRows(t, controlDB, name); len(rows) != len(expectedEvents) {
				return fmt.Errorf("%s handled rows=%d want %d", name, len(rows), len(expectedEvents))
			}
		}
		return nil
	})
	daemon2.stop(t)

	expectedSet := make(map[int64]struct{}, len(expectedEvents))
	for _, ev := range expectedEvents {
		expectedSet[ev.GlobalPosition] = struct{}{}
	}

	for _, name := range []string{txEvent1Name, txEvent2Name, txBatchName} {
		rows := getHandledRows(t, controlDB, name)
		seen := make(map[int64]int, len(rows))
		for _, row := range rows {
			seen[row.GlobalPosition]++
			if _, ok := expectedSet[row.GlobalPosition]; !ok {
				t.Fatalf("%s handled rolled-back/unexpected position %d", name, row.GlobalPosition)
			}
		}
		if len(seen) != len(expectedSet) {
			t.Fatalf("%s processed %d distinct events, want %d", name, len(seen), len(expectedSet))
		}
		for pos, count := range seen {
			if count != 1 {
				t.Fatalf("%s handled position %d %d times, want exactly 1", name, pos, count)
			}
		}
	}

	assertDetachedCovered := func(name string, appliedFn func(int64) int) {
		for pos := range expectedSet {
			if appliedFn(pos) == 0 {
				t.Fatalf("%s missed committed position %d", name, pos)
			}
		}
		for _, pos := range abortedPositions {
			if appliedFn(pos) != 0 {
				t.Fatalf("%s applied rolled-back position %d", name, pos)
			}
		}
	}
	assertDetachedCovered(detEvent1Name, func(p int64) int { return det1A.timesApplied(p) + det1B.timesApplied(p) })
	assertDetachedCovered(detEvent2Name, func(p int64) int { return det2A.timesApplied(p) + det2B.timesApplied(p) })
	assertDetachedCovered(detBatchName, func(p int64) int { return detBA.timesApplied(p) + detBB.timesApplied(p) })
}

func TestMVCC_PgBouncerTransactionPoolingSimulation(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	poolCfg := controlDB.Config().Copy()
	poolCfg.MaxConns = 4
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	bouncerPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("create transaction-pooling simulation pool: %v", err)
	}
	defer bouncerPool.Close()

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	options := append(defaultProjectorOptions(),
		projectorpkg.WithLeaderStrategy(projectorpkg.LeaderStrategyLease),
		projectorpkg.WithDispatcherStrategy(projectorpkg.DispatcherStrategyPoll),
	)

	makeReg := func(label string) *projectorpkg.Registry {
		r := projectorpkg.NewRegistry()
		if err := r.AddProjection(newTestProjection("pgbouncer-p1", label, nil)); err != nil {
			t.Fatalf("register p1: %v", err)
		}
		if err := r.AddBatch(&batchTestProjection{name: "pgbouncer-p2", instanceLabel: label}); err != nil {
			t.Fatalf("register p2: %v", err)
		}
		return r
	}

	daemon1 := startTestProjectorWithPool(t, "pgb-1", bouncerPool, makeReg("pgb-1"), options...)
	daemon2 := startTestProjectorWithPool(t, "pgb-2", bouncerPool, makeReg("pgb-2"), options...)

	firstBatch := appendTestEvents(t, controlDB, eventStore, 4, "Order")
	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
	held.Rollback(t)
	secondBatch := appendTestEvents(t, controlDB, eventStore, 4, "Order")
	midLatest := cursorOf(secondBatch)

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"pgbouncer-p1", "pgbouncer-p2"} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != midLatest {
				return fmt.Errorf("%s checkpoint=%+v want %+v", name, checkpoint, midLatest)
			}
		}
		return nil
	})

	// Verify lease failover and continued SimpleProtocol operation under the 4-conn cap.
	daemon1.stop(t)
	thirdBatch := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	finalLatest := cursorOf(thirdBatch)
	wantTotal := len(firstBatch) + len(secondBatch) + len(thirdBatch)

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{"pgbouncer-p1", "pgbouncer-p2"} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != finalLatest {
				return fmt.Errorf("%s checkpoint=%+v want %+v", name, checkpoint, finalLatest)
			}
			if rows := getHandledRows(t, controlDB, name); len(rows) != wantTotal {
				return fmt.Errorf("%s rows=%d want %d", name, len(rows), wantTotal)
			}
		}
		return nil
	})

	daemon2.stop(t)
}

func TestMVCC_DispatcherWakesOnBlockerRollback(t *testing.T) {
	t.Run("poll", func(t *testing.T) {
		controlDB := openTestDB(t)
		defer controlDB.Close()
		setupSchema(t, controlDB)
		defer cleanupTables(t, controlDB)

		eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
		projection := newTestProjection("wake-poll-rollback", "projector-1", nil)
		harness := startTestProjector(t, "projector-1", []*testProjection{projection},
			append(defaultProjectorOptions(),
				projectorpkg.WithDispatcherStrategy(projectorpkg.DispatcherStrategyPoll),
				projectorpkg.WithPollInterval(10*time.Second),
				projectorpkg.WithMaxPollInterval(10*time.Second),
				projectorpkg.WithDispatcherInterval(50*time.Millisecond),
			)...,
		)
		defer harness.stop(t)

		waitForErr(t, defaultWaitTimeout, func() error {
			assignments := getAssignments(t, controlDB)
			if len(assignments) != 1 || !assignments[0].Assigned {
				return errors.New("projection not assigned yet")
			}
			return nil
		})
		time.Sleep(250 * time.Millisecond)

		held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
		later := appendTestEvents(t, controlDB, eventStore, 2, "Order")
		latest := cursorOf(later)

		time.Sleep(250 * time.Millisecond)
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
			t.Fatalf("handled rows=%d while open blocker holds xmin, want 0", len(rows))
		}

		held.Rollback(t)
		start := time.Now()

		waitForErr(t, 2*time.Second, func() error {
			if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
				return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
			}
			if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 2 {
				return fmt.Errorf("rows=%d want 2", len(rows))
			}
			return nil
		})

		if elapsed := time.Since(start); elapsed >= 2*time.Second {
			t.Fatalf("poll dispatcher wakeup took %v after blocker rollback", elapsed)
		}
	})

	t.Run("notify", func(t *testing.T) {
		controlDB := openTestDB(t)
		defer controlDB.Close()
		setupSchema(t, controlDB)
		defer cleanupTables(t, controlDB)

		const notifyChannel = "mvcc_rollback_events"
		eventStore := storepostgres.NewStore(storepostgres.NewStoreConfig(storepostgres.WithNotifyChannel(notifyChannel)))
		projection := newTestProjection("wake-notify-rollback", "projector-1", nil)
		harness := startTestProjector(t, "projector-1", []*testProjection{projection},
			append(defaultProjectorOptions(),
				projectorpkg.WithDispatcherStrategy(projectorpkg.DispatcherStrategyNotify),
				projectorpkg.WithNotifyConnectionString(controlDB.Config().ConnString()),
				projectorpkg.WithNotifyChannel(notifyChannel),
				projectorpkg.WithPollInterval(10*time.Second),
				projectorpkg.WithMaxPollInterval(10*time.Second),
			)...,
		)
		defer harness.stop(t)

		waitForErr(t, defaultWaitTimeout, func() error {
			assignments := getAssignments(t, controlDB)
			if len(assignments) != 1 || !assignments[0].Assigned {
				return errors.New("projection not assigned yet")
			}
			return nil
		})
		time.Sleep(300 * time.Millisecond)

		held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Order", Count: 1})
		later := appendTestEvents(t, controlDB, eventStore, 2, "Order")
		latest := cursorOf(later)

		// The NOTIFY triggered by later's commit ran while held still pinned xmin,
		// so rows stay blocked until held rolls back and NotifyDispatcher reconciles.
		time.Sleep(300 * time.Millisecond)
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 0 {
			t.Fatalf("handled rows=%d while open blocker holds xmin, want 0", len(rows))
		}

		held.Rollback(t)
		start := time.Now()

		waitForErr(t, 3*time.Second, func() error {
			if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
				return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
			}
			if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 2 {
				return fmt.Errorf("rows=%d want 2", len(rows))
			}
			return nil
		})

		if elapsed := time.Since(start); elapsed >= 3*time.Second {
			t.Fatalf("notify dispatcher reconciliation took %v, expected well below 10s PollInterval", elapsed)
		}
	})
}

func TestMVCC_DetachedContentionAndPhaseTimeouts(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{
		name:          "mvcc-detached-phase",
		entered:       make(chan struct{}, 64),
		applyDelay:    450 * time.Millisecond,
		delayAttempts: 2,
	}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(),
		projectorpkg.WithMaxConsecutiveFailures(0),
		projectorpkg.WithDetachedApplyTimeout(150*time.Millisecond),
		projectorpkg.WithCheckpointTimeout(time.Second),
		projectorpkg.WithPollInterval(50*time.Millisecond),
	)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	held := beginControlledAppend(t, controlDB, eventStore, testEventBatch{StreamType: "Product", Count: 1})
	committed := appendTestEvents(t, controlDB, eventStore, 2, "Product")
	latest := cursorOf(committed)

	time.Sleep(300 * time.Millisecond)
	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); !checkpoint.IsZero() {
		t.Fatalf("checkpoint=%+v want zero while open write pins xmin", checkpoint)
	}

	held.Commit(t)

	// Wait for all 3 events (held + 2 committed) after initial DetachedApplyTimeout retries settle.
	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		if applied := projection.uniqueApplied(); applied != 3 {
			return fmt.Errorf("unique applied=%d want 3", applied)
		}
		return nil
	})

	harness.stop(t)
}

func TestMVCC_UpgradeFromV060Runbook(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	ctx := context.Background()
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	// Replace v0.7.0 projector tables with the exact v0.6.0 schema (including projection_gap_skips
	// and projection_checkpoints without last_transaction_id).
	if _, err := controlDB.Exec(ctx, `
DROP TABLE IF EXISTS projection_gap_skips, projection_checkpoints, projection_assignments, projector_leader_leases, projector_instances CASCADE;

CREATE TABLE projector_instances (
	instance_id UUID PRIMARY KEY,
	heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE projection_assignments (
	projection_name TEXT PRIMARY KEY,
	instance_id UUID REFERENCES projector_instances(instance_id) ON DELETE SET NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE projection_checkpoints (
	projection_name TEXT PRIMARY KEY,
	last_position BIGINT NOT NULL DEFAULT 0,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE projector_leader_leases (
	lease_key TEXT PRIMARY KEY,
	leader_id UUID REFERENCES projector_instances(instance_id) ON DELETE CASCADE,
	expires_at TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE projection_gap_skips (
	id BIGSERIAL PRIMARY KEY,
	projection_name TEXT NOT NULL,
	gap_position BIGINT NOT NULL,
	skip_to_position BIGINT NOT NULL,
	highest_visible_position BIGINT NOT NULL,
	stale_duration_ms BIGINT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`); err != nil {
		t.Fatalf("install v0.6.0 schema: %v", err)
	}

	// Simulate two pre-v0.3 legacy rows backfilled to transaction_id='0', plus one live v0.3 row.
	legacyEvents := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	if _, err := controlDB.Exec(ctx, `UPDATE events SET transaction_id = '0'::xid8 WHERE global_position <= $1`, legacyEvents[1].GlobalPosition); err != nil {
		t.Fatalf("backfill legacy transaction_id: %v", err)
	}
	liveBeforeUpgrade := appendTestEvents(t, controlDB, eventStore, 1, "Order")

	if _, err := controlDB.Exec(ctx, `
INSERT INTO projection_checkpoints (projection_name, last_position, created_at, updated_at) VALUES
	('upgrade-zero', 0, NOW(), NOW()),
	('upgrade-legacy-xid0', $1, NOW(), NOW()),
	('upgrade-live-xid', $2, NOW(), NOW())
`, legacyEvents[0].GlobalPosition, liveBeforeUpgrade[0].GlobalPosition); err != nil {
		t.Fatalf("seed v0.6.0 checkpoints: %v", err)
	}
	if _, err := controlDB.Exec(ctx, `
INSERT INTO projection_gap_skips (projection_name, gap_position, skip_to_position, highest_visible_position, stale_duration_ms)
VALUES ('upgrade-live-xid', 1, 2, 3, 5000)
`); err != nil {
		t.Fatalf("seed v0.6.0 gap skip row: %v", err)
	}

	// Execute the exact upgrade SQL documented in projector/UPGRADING.md.
	upgradeStatements := []string{
		`ALTER TABLE projection_checkpoints ADD COLUMN IF NOT EXISTS last_transaction_id xid8 NOT NULL DEFAULT '0'`,
		`UPDATE projection_checkpoints pc
SET last_transaction_id = COALESCE(
	(SELECT e.transaction_id FROM events e WHERE e.global_position = pc.last_position),
	'0'::xid8
)
WHERE pc.last_position > 0 AND pc.last_transaction_id = '0'::xid8`,
		`DROP TABLE IF EXISTS projection_gap_skips`,
	}
	for _, stmt := range upgradeStatements {
		if _, err := controlDB.Exec(ctx, stmt); err != nil {
			t.Fatalf("run v0.6.0 -> v0.7.0 upgrade SQL %q: %v", stmt, err)
		}
	}

	var gapSkipExists int
	if err := controlDB.QueryRow(ctx, `
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = 'projection_gap_skips'
`).Scan(&gapSkipExists); err != nil {
		t.Fatalf("check projection_gap_skips removal: %v", err)
	}
	if gapSkipExists != 0 {
		t.Fatalf("projection_gap_skips still exists after upgrade SQL")
	}

	afterUpgrade := appendTestEvents(t, controlDB, eventStore, 2, "Order")
	finalCursor := cursorOf(afterUpgrade)

	pZero := newTestProjection("upgrade-zero", "projector-1", nil)
	pLegacy := newTestProjection("upgrade-legacy-xid0", "projector-1", nil)
	pLive := newTestProjection("upgrade-live-xid", "projector-1", nil)

	harness := startTestProjector(t, "projector-1", []*testProjection{pZero, pLegacy, pLive}, defaultProjectorOptions()...)

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, p := range []*testProjection{pZero, pLegacy, pLive} {
			if checkpoint := getCheckpoint(t, controlDB, p.Name()); checkpoint != finalCursor {
				return fmt.Errorf("%s checkpoint=%+v want %+v", p.Name(), checkpoint, finalCursor)
			}
		}
		return nil
	})
	harness.stop(t)

	if rows := getHandledRows(t, controlDB, pZero.Name()); len(rows) != 5 {
		t.Fatalf("upgrade-zero handled %d rows, want 5", len(rows))
	}
	legacyRows := getHandledRows(t, controlDB, pLegacy.Name())
	if len(legacyRows) != 4 || legacyRows[0].GlobalPosition != legacyEvents[1].GlobalPosition {
		t.Fatalf("upgrade-legacy-xid0 handled %d rows (first=%v), want 4 starting at pos %d", len(legacyRows), legacyRows, legacyEvents[1].GlobalPosition)
	}
	liveRows := getHandledRows(t, controlDB, pLive.Name())
	if len(liveRows) != 2 || liveRows[0].GlobalPosition != afterUpgrade[0].GlobalPosition {
		t.Fatalf("upgrade-live-xid handled %d rows (first=%v), want 2 starting at pos %d", len(liveRows), liveRows, afterUpgrade[0].GlobalPosition)
	}
}
