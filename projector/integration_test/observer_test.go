//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"

	storepostgres "eventsalsa.dev/store/postgres"

	projectorpkg "eventsalsa.dev/projector"
)

type testIntegrationObserver struct {
	mu         sync.Mutex
	batches    []projectorpkg.BatchStats
	heartbeats []projectorpkg.DaemonStats
	degraded   []projectorpkg.DegradedStats
	poison     []projectorpkg.PoisonBatchStats
	rebalances []map[string]uuid.UUID
}

//nolint:gocritic // hugeParam: test mock
func (o *testIntegrationObserver) OnBatchProcessed(_ context.Context, stats projectorpkg.BatchStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batches = append(o.batches, stats)
}

func (o *testIntegrationObserver) OnHeartbeat(_ context.Context, stats projectorpkg.DaemonStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.heartbeats = append(o.heartbeats, stats)
}

func (o *testIntegrationObserver) OnProjectionDegraded(_ context.Context, stats projectorpkg.DegradedStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.degraded = append(o.degraded, stats)
}

func (o *testIntegrationObserver) OnPoisonBatchSkipped(_ context.Context, stats projectorpkg.PoisonBatchStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.poison = append(o.poison, stats)
}

func (o *testIntegrationObserver) OnRebalance(_ context.Context, assignments map[string]uuid.UUID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	copied := make(map[string]uuid.UUID, len(assignments))
	for k, v := range assignments {
		copied[k] = v
	}
	o.rebalances = append(o.rebalances, copied)
}

func (o *testIntegrationObserver) Batches() []projectorpkg.BatchStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.BatchStats(nil), o.batches...)
}

func (o *testIntegrationObserver) Heartbeats() []projectorpkg.DaemonStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.DaemonStats(nil), o.heartbeats...)
}

func (o *testIntegrationObserver) Degraded() []projectorpkg.DegradedStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.DegradedStats(nil), o.degraded...)
}

func (o *testIntegrationObserver) PoisonSkipped() []projectorpkg.PoisonBatchStats {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]projectorpkg.PoisonBatchStats(nil), o.poison...)
}

func (o *testIntegrationObserver) Rebalances() []map[string]uuid.UUID {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]map[string]uuid.UUID, len(o.rebalances))
	for i, r := range o.rebalances {
		copied := make(map[string]uuid.UUID, len(r))
		for k, v := range r {
			copied[k] = v
		}
		out[i] = copied
	}
	return out
}

func TestIntegration_Observer_FullLifecycle(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-observer-lifecycle", "projector-1", nil)
	observer := &testIntegrationObserver{}

	projector := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		assignments := getAssignments(t, controlDB)
		if len(assignments) != 1 || !assignments[0].Assigned {
			return fmt.Errorf("projection not assigned yet")
		}
		return nil
	})

	appended := appendTestEvents(t, controlDB, eventStore, 5, "Account")
	latest := cursorOf(appended)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		batches := observer.Batches()
		if len(batches) == 0 {
			return fmt.Errorf("no batch stats observed yet")
		}
		lastBatch := batches[len(batches)-1]
		if lastBatch.LastPosition != latest.GlobalPosition || lastBatch.LastTransactionID != latest.TransactionID {
			return fmt.Errorf("last batch cursor=(%d,%d) want %+v", lastBatch.LastTransactionID, lastBatch.LastPosition, latest)
		}
		if lastBatch.StartTransactionID != 0 || lastBatch.StartPosition != 0 {
			return fmt.Errorf("last batch start cursor=(%d,%d) want (0,0)", lastBatch.StartTransactionID, lastBatch.StartPosition)
		}
		if lastBatch.HeadPosition != latest.GlobalPosition || lastBatch.HeadTransactionID != latest.TransactionID {
			return fmt.Errorf("last batch head=(%d,%d) want %+v", lastBatch.HeadTransactionID, lastBatch.HeadPosition, latest)
		}
		if lastBatch.EventsHandled != 5 {
			return fmt.Errorf("last batch handled=%d want 5", lastBatch.EventsHandled)
		}
		if lastBatch.Error != nil {
			return fmt.Errorf("batch error = %v, want nil", lastBatch.Error)
		}
		return nil
	})

	waitForErr(t, defaultWaitTimeout, func() error {
		heartbeats := observer.Heartbeats()
		if len(heartbeats) == 0 {
			return fmt.Errorf("expected heartbeats in observer")
		}
		if heartbeats[0].InstanceID != projector.daemon.ID() {
			return fmt.Errorf("heartbeat InstanceID = %v, want %v", heartbeats[0].InstanceID, projector.daemon.ID())
		}
		rebalances := observer.Rebalances()
		if len(rebalances) == 0 {
			return fmt.Errorf("expected rebalances in observer")
		}
		if rebalances[0][projection.Name()] != projector.daemon.ID() {
			return fmt.Errorf("rebalance mapping for %s = %v, want %v", projection.Name(), rebalances[0][projection.Name()], projector.daemon.ID())
		}
		return nil
	})
}

func TestIntegration_Observer_BacklogCatchupLag(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	// Seed 60 events prior to starting projector
	appended := appendTestEvents(t, controlDB, eventStore, 60, "Invoice")
	latest := cursorOf(appended)

	projection := newTestProjection("projection-observer-lag", "projector-1", nil)
	observer := &testIntegrationObserver{}

	_ = startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithBatchSize(20),
			projectorpkg.WithObserver(observer),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		batches := observer.Batches()
		if len(batches) < 3 {
			return fmt.Errorf("expected at least 3 batches for 60 events with batch size 20, got %d", len(batches))
		}
		return nil
	})

	batches := observer.Batches()
	for i, b := range batches {
		if b.Error != nil {
			t.Fatalf("batch %d error: %v", i, b.Error)
		}
		if b.Duration <= 0 {
			t.Fatalf("batch %d duration = %v, want > 0", i, b.Duration)
		}
	}
	if batches[0].Lag == 0 {
		t.Fatalf("first batch Lag = 0, want > 0 during backlog catch-up")
	}
	// Last batch should be caught up
	last := batches[len(batches)-1]
	if last.LastPosition != latest.GlobalPosition || last.LastTransactionID != latest.TransactionID {
		t.Fatalf("last batch cursor = (%d,%d), want %+v", last.LastTransactionID, last.LastPosition, latest)
	}
	if last.HeadPosition != latest.GlobalPosition || last.HeadTransactionID != latest.TransactionID {
		t.Fatalf("last batch head = (%d,%d), want %+v", last.HeadTransactionID, last.HeadPosition, latest)
	}
	if last.Lag != 0 {
		t.Fatalf("last batch Lag = %d, want 0 once caught up", last.Lag)
	}
}

func TestIntegration_Observer_TransactionIDsAndHeadLag(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	firstTx := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	secondTx := appendTestEvents(t, controlDB, eventStore, 3, "Order")
	latest := cursorOf(secondTx)

	projection := newTestProjection("projection-observer-xid", "projector-1", nil)
	observer := &testIntegrationObserver{}

	harness := startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(),
			projectorpkg.WithBatchSize(3),
			projectorpkg.WithObserver(observer),
		)...,
	)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != latest {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, latest)
		}
		batches := observer.Batches()
		if len(batches) < 2 {
			return fmt.Errorf("observed %d batches, want at least 2", len(batches))
		}
		return nil
	})
	harness.stop(t)

	batches := observer.Batches()
	firstBatch := batches[0]
	if firstBatch.StartTransactionID != 0 || firstBatch.StartPosition != 0 {
		t.Fatalf("first batch start = (%d,%d), want (0,0)", firstBatch.StartTransactionID, firstBatch.StartPosition)
	}
	if firstBatch.LastTransactionID != firstTx[2].TransactionID || firstBatch.LastPosition != firstTx[2].GlobalPosition {
		t.Fatalf("first batch end = (%d,%d), want (%d,%d)",
			firstBatch.LastTransactionID, firstBatch.LastPosition,
			firstTx[2].TransactionID, firstTx[2].GlobalPosition)
	}
	if firstBatch.HeadTransactionID != latest.TransactionID || firstBatch.HeadPosition != latest.GlobalPosition {
		t.Fatalf("first batch head = (%d,%d), want %+v",
			firstBatch.HeadTransactionID, firstBatch.HeadPosition, latest)
	}
	wantFirstLag := secondTx[2].GlobalPosition - firstTx[2].GlobalPosition
	if firstBatch.Lag != wantFirstLag {
		t.Fatalf("first batch Lag = %d, want %d", firstBatch.Lag, wantFirstLag)
	}

	lastBatch := batches[len(batches)-1]
	if lastBatch.StartTransactionID != firstTx[2].TransactionID || lastBatch.StartPosition != firstTx[2].GlobalPosition {
		t.Fatalf("last batch start = (%d,%d), want (%d,%d)",
			lastBatch.StartTransactionID, lastBatch.StartPosition,
			firstTx[2].TransactionID, firstTx[2].GlobalPosition)
	}
	if lastBatch.LastTransactionID != latest.TransactionID || lastBatch.LastPosition != latest.GlobalPosition {
		t.Fatalf("last batch end = (%d,%d), want %+v",
			lastBatch.LastTransactionID, lastBatch.LastPosition, latest)
	}
	if lastBatch.Lag != 0 {
		t.Fatalf("last batch Lag = %d, want 0", lastBatch.Lag)
	}
}

func TestIntegration_Observer_BatchFailureAndRecovery(t *testing.T) {
	controlDB := openTestDB(t)
	defer controlDB.Close()
	setupSchema(t, controlDB)
	defer cleanupTables(t, controlDB)

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	projection := newTestProjection("projection-observer-retry", "projector-1", nil)
	observer := &testIntegrationObserver{}

	_ = startTestProjector(t, "projector-1", []*testProjection{projection},
		append(defaultProjectorOptions(), projectorpkg.WithObserver(observer))...,
	)

	appended := appendTestEvents(t, controlDB, eventStore, 3, "Customer")
	failingPos := appended[1].GlobalPosition
	lastCursor := cursorOf(appended)

	projection.FailUntilCleared(failingPos, errors.New("simulated transient handler failure"))

	// Wait until failure is observed
	waitForErr(t, defaultWaitTimeout, func() error {
		batches := observer.Batches()
		for _, b := range batches {
			if b.Error != nil {
				return nil
			}
		}
		return fmt.Errorf("no failed batch observed yet")
	})

	// Clear failure and verify recovery
	projection.ClearFailure(failingPos)

	waitForErr(t, defaultWaitTimeout, func() error {
		if checkpoint := getCheckpoint(t, controlDB, projection.Name()); checkpoint != lastCursor {
			return fmt.Errorf("checkpoint=%+v want %+v", checkpoint, lastCursor)
		}
		batches := observer.Batches()
		last := batches[len(batches)-1]
		if last.Error != nil {
			return fmt.Errorf("last batch still has error: %v", last.Error)
		}
		if last.LastPosition != lastCursor.GlobalPosition || last.LastTransactionID != lastCursor.TransactionID {
			return fmt.Errorf("last batch cursor=(%d,%d) want %+v", last.LastTransactionID, last.LastPosition, lastCursor)
		}
		return nil
	})
}
