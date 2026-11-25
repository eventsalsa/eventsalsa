//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
	"eventsalsa.dev/store/postgres"
)

// TestExpectedVersion_NoStream tests that NoStream() enforces stream doesn't exist
func TestExpectedVersion_NoStream(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()

	event := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	// First append with NoStream() should succeed
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{event})
	if err != nil {
		t.Fatalf("First append with NoStream() should succeed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second append with NoStream() should fail (stream already exists)
	event2 := event
	event2.EventID = uuid.New()
	event2.EventType = "TestEventUpdated"

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.NoStream(), []store.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second append with NoStream() should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback(ctx)
}

// TestExpectedVersion_Exact tests that Exact(N) enforces exact version match
func TestExpectedVersion_Exact(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()

	// Create stream with version 1
	event1 := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{event1})
	if err != nil {
		t.Fatalf("First append should succeed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Append with Exact(1) should succeed
	event2 := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventUpdated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Exact(1), []store.Event{event2})
	if err != nil {
		t.Fatalf("Append with Exact(1) should succeed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Append with Exact(1) should now fail (version is now 2)
	event3 := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventUpdated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx3, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx3, store.Exact(1), []store.Event{event3})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Append with Exact(1) should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx3.Rollback(ctx)

	// Append with Exact(2) should succeed
	tx4, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx4, store.Exact(2), []store.Event{event3})
	if err != nil {
		t.Fatalf("Append with Exact(2) should succeed: %v", err)
	}
	if err := tx4.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

// TestExpectedVersion_Exact_NonExistent tests that Exact(N) fails for non-existent streams
func TestExpectedVersion_Exact_NonExistent(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()

	event := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	// Append with Exact(1) should fail (stream doesn't exist)
	tx, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx, store.Exact(1), []store.Event{event})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Append with Exact(1) on non-existent stream should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx.Rollback(ctx)
}

// TestExpectedVersion_Exact_Zero tests that Exact(0) can be used to create new streams
func TestExpectedVersion_Exact_Zero(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()

	event := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	// Append with Exact(0) should succeed when stream is at version 0 (new stream)
	tx1, _ := db.BeginTx(ctx, nil)
	result, err := pgStore.Append(ctx, tx1, store.Exact(0), []store.Event{event})
	if err != nil {
		t.Fatalf("First append with Exact(0) should succeed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify stream now has version 1
	if result.ToVersion() != 1 {
		t.Errorf("Expected version 1 after first append, got %d", result.ToVersion())
	}

	// Append with Exact(0) should now fail (stream is at version 1, not 0)
	event2 := event
	event2.EventID = uuid.New()
	event2.EventType = "TestEventUpdated"

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Exact(0), []store.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second append with Exact(0) should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback(ctx)
}

// TestExpectedVersion_Any tests that Any() allows appends regardless of version
func TestExpectedVersion_Any(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	streamID := uuid.New().String()

	// First append with Any() on new stream should succeed
	event1 := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventCreated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.Any(), []store.Event{event1})
	if err != nil {
		t.Fatalf("First append with Any() should succeed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second append with Any() on existing stream should also succeed
	event2 := store.Event{
		StreamType:   "TestStream",
		StreamID:     streamID,
		EventID:      uuid.New(),
		EventType:    "TestEventUpdated",
		EventVersion: 1,
		Payload:      []byte(`{}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.Any(), []store.Event{event2})
	if err != nil {
		t.Fatalf("Second append with Any() should succeed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
}

// TestExpectedVersion_UniquenessPattern tests the reservation stream pattern
func TestExpectedVersion_UniquenessPattern(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	setupTestTables(t, db)

	ctx := context.Background()
	pgStore := postgres.NewStore(postgres.DefaultStoreConfig())

	// Use email as stream ID (reservation pattern)
	email := "user@example.com"

	event := store.Event{
		StreamType:   "EmailReservation",
		StreamID:     email,
		EventID:      uuid.New(),
		EventType:    "EmailReserved",
		EventVersion: 1,
		Payload:      []byte(`{"email":"user@example.com"}`),
		Metadata:     []byte(`{}`),
		CreatedAt:    time.Now(),
	}

	// First reservation should succeed
	tx1, _ := db.BeginTx(ctx, nil)
	_, err := pgStore.Append(ctx, tx1, store.NoStream(), []store.Event{event})
	if err != nil {
		t.Fatalf("First email reservation should succeed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Second reservation of same email should fail (enforces uniqueness)
	event2 := event
	event2.EventID = uuid.New()

	tx2, _ := db.BeginTx(ctx, nil)
	_, err = pgStore.Append(ctx, tx2, store.NoStream(), []store.Event{event2})
	if err != store.ErrOptimisticConcurrency {
		t.Fatalf("Second email reservation should fail with ErrOptimisticConcurrency, got: %v", err)
	}
	tx2.Rollback(ctx)
}

func TestExpectedVersion_NoStreamConflictKeepsTxUsable(t *testing.T) {
	db := openSimpleProtocolTestDB(t)
	ctx := context.Background()
	es := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := "nostream-" + uuid.NewString()

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	if _, appendErr := es.Append(ctx, tx1, store.NoStream(), []store.Event{
		makeSimpleEvent(streamID, "Created", uuid.New()),
	}); appendErr != nil {
		t.Fatalf("seed NoStream: %v", appendErr)
	}
	if commitErr := tx1.Commit(ctx); commitErr != nil {
		t.Fatalf("commit tx1: %v", commitErr)
	}

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() {
		if rbErr := tx2.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			t.Errorf("rollback tx2: %v", rbErr)
		}
	}()

	_, err = es.Append(ctx, tx2, store.NoStream(), []store.Event{
		makeSimpleEvent(streamID, "DuplicateCreate", uuid.New()),
	})
	if !errors.Is(err, store.ErrOptimisticConcurrency) {
		t.Fatalf("NoStream conflict err = %v, want ErrOptimisticConcurrency", err)
	}

	var probe int
	if qErr := tx2.QueryRow(ctx, "SELECT 1").Scan(&probe); qErr != nil || probe != 1 {
		t.Fatalf("tx2 poisoned (expected no 25P02) after NoStream conflict: probe=%d err=%v", probe, qErr)
	}
}

func TestExpectedVersion_MatchesAndConcurrentAnyAppends(t *testing.T) {
	if !store.NoStream().Matches(0) || store.NoStream().Matches(1) {
		t.Fatal("NoStream().Matches broken")
	}
	if !store.Exact(3).Matches(3) || store.Exact(3).Matches(2) {
		t.Fatal("Exact(3).Matches broken")
	}
	if !store.Any().Matches(0) || !store.Any().Matches(99) {
		t.Fatal("Any().Matches broken")
	}

	db := openSimpleProtocolTestDB(t)
	ctx := context.Background()
	es := postgres.NewStore(postgres.DefaultStoreConfig())
	streamID := "any-occ-" + uuid.NewString()

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Go(func() {
			tx, bErr := db.BeginTx(ctx, nil)
			if bErr != nil {
				errs <- bErr
				return
			}
			_, aErr := es.Append(ctx, tx, store.Any(), []store.Event{
				makeSimpleEvent(streamID, fmt.Sprintf("E%d", i), uuid.New()),
			})
			if aErr != nil {
				if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
					errs <- rbErr
					return
				}
				errs <- aErr
				return
			}
			errs <- tx.Commit(ctx)
		})
	}
	wg.Wait()
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent Any() append failed: %v", callErr)
		}
	}

	txCheck, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin check tx: %v", err)
	}
	defer func() {
		if rbErr := txCheck.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			t.Errorf("rollback check tx: %v", rbErr)
		}
	}()

	if _, exactErr := es.Append(ctx, txCheck, store.Exact(workers-1), []store.Event{
		makeSimpleEvent(streamID, "Stale", uuid.New()),
	}); !errors.Is(exactErr, store.ErrOptimisticConcurrency) {
		t.Fatalf("Exact mismatch err = %v, want ErrOptimisticConcurrency", exactErr)
	}
}
