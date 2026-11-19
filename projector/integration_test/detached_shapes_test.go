//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"

	projectorpkg "eventsalsa.dev/projector"
)

func TestDetachedProjection_FilterWithNoMatchesStillAdvancesCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-nomatch", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection, projectorpkg.OnStreamTypes("Nonexistent")); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Product")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		return nil
	})

	if applied := projection.uniqueApplied(); applied != 0 {
		t.Fatalf("detached applied %d events, want 0 for a filter that matches nothing", applied)
	}

	harness.stop(t)
}

func TestDetachedProjection_FilterOnlyAppliesMatchingEvents(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-filter", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection, projectorpkg.OnStreamTypes("Product")); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Product", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		return nil
	})

	if applied := projection.uniqueApplied(); applied != 2 {
		t.Fatalf("detached applied %d events, want 2 matching Product events", applied)
	}

	harness.stop(t)
}

func TestDetachedBatchProjection_AppliesWindowInOneCall(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedBatchProjection{name: "detached-batch-bulk"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetachedBatch(projection); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Product")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		return nil
	})

	if calls := projection.callsCount(); calls != 1 {
		t.Fatalf("batch calls = %d, want 1", calls)
	}
	history := projection.batchSizeHistory()
	if len(history) != 1 || history[0] != 5 {
		t.Fatalf("batch sizes = %v, want [5]", history)
	}

	harness.stop(t)
}

func TestDetachedProjection_PerEventPartialFailureKeepsCheckpoint(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &scriptedDetachedProjection{name: "detached-partial", entered: make(chan struct{}, 64)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 4, "Product")
	latest := cursorOf(appended)
	failing := appended[2].GlobalPosition
	projection.failAt(failing, -1, errors.New("destination rejected document"))

	waitForErr(t, defaultWaitTimeout, func() error {
		if projection.timesApplied(appended[0].GlobalPosition) == 0 {
			return errors.New("handler was not attempted yet")
		}
		return nil
	})

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); !checkpoint.IsZero() {
		t.Fatalf("checkpoint=%+v want zero while the batch has a failing event", checkpoint)
	}

	projection.failAt(failing, 0, nil)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v after recovery", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestDetachedProjection_DoesNotHoldAssignmentLockWhileApplying(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	gate := make(chan struct{})
	defer close(gate)
	projection := &scriptedDetachedProjection{name: "detached-lock", gate: gate, entered: make(chan struct{}, 8)}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddDetached(projection); err != nil {
		t.Fatalf("register detached projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithMaxConsecutiveFailures(0))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return errors.New("projection not assigned yet")
		}
		return nil
	})

	appendTestEvents(t, controlDB, eventStore, 1, "Product")
	waitForErr(t, defaultWaitTimeout, func() error {
		select {
		case <-projection.entered:
			return nil
		default:
			return errors.New("detached handler never entered")
		}
	})

	ctx := context.Background()
	conn, err := controlDB.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	if _, err := lockTx.Exec(ctx, "SET LOCAL lock_timeout = '1s'"); err != nil {
		t.Fatalf("set lock timeout: %v", err)
	}

	var owner *uuid.UUID
	if err := lockTx.QueryRow(ctx,
		`SELECT instance_id FROM projection_assignments WHERE projection_name = $1 FOR UPDATE`,
		projection.Name()).Scan(&owner); err != nil {
		t.Fatalf("assignment row was locked while the detached handler was blocked: %v", err)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release assignment lock: %v", err)
	}
	conn.Release()

	harness.stop(t)
}

func TestBatchProjection_RespectsWindowBoundary(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := &batchTestProjection{name: "batch-window", instanceLabel: "projector-1"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddBatch(projection); err != nil {
		t.Fatalf("register batch projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithBatchSize(2))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Order")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		return nil
	})

	if history := projection.BatchSizes(); len(history) < 3 {
		t.Fatalf("batch sizes = %v, want at least 3 batches for 5 events with window 2", history)
	}

	harness.stop(t)
}

func TestRegistry_FromProjectionsKeepsClassicBehavior(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("classic-projection", "projector-1", nil)

	registry := projectorpkg.FromProjections(projection)
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		if rows := getHandledRows(t, controlDB, projection.Name()); len(rows) != 3 {
			return fmt.Errorf("handled rows = %d, want 3", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistry_DeprecatedFilterWrapperStillRegisters(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("wrapped-projection", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projectorpkg.FilterStreamTypes(projection, "Order")); err != nil {
		t.Fatalf("register wrapped projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 2},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 2 {
			return fmt.Errorf("handled rows = %d, want 2 Order events", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_CombinedStreamAndEventTypes(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("combined-filter", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection,
		projectorpkg.OnStreamTypes("Order"),
		projectorpkg.OnEventTypes("Order.event.1"),
	); err != nil {
		t.Fatalf("register filtered projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 3},
		testEventBatch{StreamType: "User", Count: 2},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		rows := getHandledRows(t, controlDB, projection.Name())
		if len(rows) != 1 {
			return fmt.Errorf("handled rows = %d, want 1", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_TwoProjectionsDifferentFilters(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	orders := newTestProjection("orders-only", "projector-1", nil)
	users := newTestProjection("users-only", "projector-1", nil)

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(orders, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register orders projection: %v", err)
	}
	if err := registry.AddProjection(users, projectorpkg.OnStreamTypes("User")); err != nil {
		t.Fatalf("register users projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, projection := range []*testProjection{orders, users} {
			if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%+v want %+v", projection.Name(), checkpoint, latest)
			}
		}
		if rows := getHandledRows(t, controlDB, orders.Name()); len(rows) != 2 {
			return fmt.Errorf("orders handled %d rows, want 2", len(rows))
		}
		if rows := getHandledRows(t, controlDB, users.Name()); len(rows) != 3 {
			return fmt.Errorf("users handled %d rows, want 3", len(rows))
		}
		return nil
	})

	harness.stop(t)
}

func TestRegistrationFilter_ObserverAccounting(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("accounted-filter", "projector-1", nil)
	observer := &testIntegrationObserver{}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(projection, projectorpkg.OnStreamTypes("Order")); err != nil {
		t.Fatalf("register filtered projection: %v", err)
	}

	options := append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))
	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, options...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "Order", Count: 2},
		testEventBatch{StreamType: "User", Count: 3},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		return nil
	})

	batches := observer.Batches()
	if len(batches) == 0 {
		t.Fatal("no batches observed")
	}

	totalHandled := 0
	reportedTwo := false
	for _, batch := range batches {
		if batch.EventsHandled > batch.EventsRead {
			t.Fatalf("batch reported EventsHandled=%d > EventsRead=%d", batch.EventsHandled, batch.EventsRead)
		}
		totalHandled += batch.EventsHandled
		if batch.EventsHandled == 2 {
			reportedTwo = true
		}
	}
	if !reportedTwo {
		t.Fatalf("no batch reported the 2 matching events; batches=%v", batches)
	}
	if totalHandled != 2 {
		t.Fatalf("total EventsHandled=%d, want 2 (filtered events must not count as handled)", totalHandled)
	}

	harness.stop(t)
}

func TestCheckpointContention_BlocksUntilLockReleased(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("contended", "projector-1", nil)

	ctx := context.Background()
	seedCheckpoint(t, controlDB, projection.Name(), store.Cursor{})

	conn, err := controlDB.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	lockTx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	var locked int64
	if err := lockTx.QueryRow(ctx,
		`SELECT last_position FROM projection_checkpoints WHERE projection_name = $1 FOR UPDATE`,
		projection.Name()).Scan(&locked); err != nil {
		t.Fatalf("lock checkpoint row: %v", err)
	}

	harness := startTestProjector(t, "projector-1", []*testProjection{projection}, defaultProjectorOptions()...)

	appended := appendTestEvents(t, controlDB, eventStore, 1, "Order")
	latest := cursorOf(appended)

	time.Sleep(700 * time.Millisecond)

	if checkpoint := getCheckpoint(t, controlDB, projection.Name()); !checkpoint.IsZero() {
		t.Fatalf("checkpoint=%+v want zero while another session holds the checkpoint row", checkpoint)
	}

	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatalf("release checkpoint lock: %v", err)
	}
	conn.Release()

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v after the lock was released", checkpoint, latest)
		}
		return nil
	})

	harness.stop(t)
}

func TestMixedShapes_AllFourRunInOneDaemon(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	txEvent := newTestProjection("mixed-tx-event", "projector-1", nil)
	txBatch := &batchTestProjection{name: "mixed-tx-batch", instanceLabel: "projector-1"}
	detEvent := &scriptedDetachedProjection{name: "mixed-det-event", entered: make(chan struct{}, 64)}
	detBatch := &scriptedDetachedBatchProjection{name: "mixed-det-batch"}

	registry := projectorpkg.NewRegistry()
	if err := registry.AddProjection(txEvent, projectorpkg.OnStreamTypes("tx-event")); err != nil {
		t.Fatalf("register transactional per-event projection: %v", err)
	}
	if err := registry.AddBatch(txBatch, projectorpkg.OnStreamTypes("tx-batch")); err != nil {
		t.Fatalf("register transactional batch projection: %v", err)
	}
	if err := registry.AddDetached(detEvent, projectorpkg.OnStreamTypes("det-event")); err != nil {
		t.Fatalf("register detached per-event projection: %v", err)
	}
	if err := registry.AddDetachedBatch(detBatch, projectorpkg.OnStreamTypes("det-batch")); err != nil {
		t.Fatalf("register detached batch projection: %v", err)
	}

	harness := startTestProjectorFromRegistry(t, "projector-1", registry, 8, defaultProjectorOptions()...)

	appended := appendTestEventBatches(t, controlDB, eventStore,
		testEventBatch{StreamType: "tx-event", Count: 1},
		testEventBatch{StreamType: "tx-batch", Count: 1},
		testEventBatch{StreamType: "det-event", Count: 1},
		testEventBatch{StreamType: "det-batch", Count: 1},
	)
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		for _, name := range []string{txEvent.Name(), txBatch.Name(), detEvent.Name(), detBatch.Name()} {
			if checkpoint := getCheckpoint(t, controlDB, name); checkpoint != latest {
				return fmt.Errorf("%s checkpoint=%+v want %+v", name, checkpoint, latest)
			}
		}

		if rows := getHandledRows(t, controlDB, txEvent.Name()); len(rows) != 1 {
			return fmt.Errorf("transactional per-event rows=%d, want 1", len(rows))
		}
		if rows := getHandledRows(t, controlDB, txBatch.Name()); len(rows) != 1 {
			return fmt.Errorf("transactional batch rows=%d, want 1", len(rows))
		}
		if applied := detEvent.uniqueApplied(); applied != 1 {
			return fmt.Errorf("detached per-event applied=%d, want 1", applied)
		}
		if calls := detBatch.callsCount(); calls != 1 {
			return fmt.Errorf("detached batch calls=%d, want 1", calls)
		}
		return nil
	})

	harness.stop(t)
}
