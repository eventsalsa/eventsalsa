package persistence_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
)

type storedRecord[S any] struct {
	state   S
	version int64
}

type genericStateRepo[S any] struct {
	mu                sync.RWMutex
	records           map[string]storedRecord[S]
	loadErr           error
	saveErr           error
	beforeSave        func(id string, expected int64)
	forceNegVersion   bool
	loadCalls         int
	saveCalls         int
	lastExpected      int64
	failNextConflicts atomic.Int32
}

type spyStateRepo = genericStateRepo[counterState]

func newGenericStateRepo[S any]() *genericStateRepo[S] {
	return &genericStateRepo[S]{records: make(map[string]storedRecord[S])}
}

func newSpyStateRepo() *spyStateRepo {
	return newGenericStateRepo[counterState]()
}

func (r *genericStateRepo[S]) Load(_ context.Context, id string) (state S, version int64, err error) {
	r.mu.Lock()
	r.loadCalls++
	loadErr := r.loadErr
	forceNeg := r.forceNegVersion
	rec, ok := r.records[id]
	r.mu.Unlock()

	if loadErr != nil {
		var zero S
		return zero, 0, loadErr
	}
	if forceNeg {
		var zero S
		return zero, -1, nil
	}
	if !ok {
		var zero S
		return zero, 0, persistence.ErrNotFound
	}
	return rec.state, rec.version, nil
}

func (r *genericStateRepo[S]) Save(_ context.Context, id string, expectedVersion int64, state S) error {
	r.mu.Lock()
	beforeSave := r.beforeSave
	r.mu.Unlock()
	if beforeSave != nil {
		beforeSave(id, expectedVersion)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveCalls++
	r.lastExpected = expectedVersion

	if r.failNextConflicts.Load() > 0 {
		r.failNextConflicts.Add(-1)
		return persistence.ErrConcurrencyConflict
	}
	if r.saveErr != nil {
		return r.saveErr
	}
	rec, exists := r.records[id]
	if expectedVersion == 0 && exists {
		return persistence.ErrConcurrencyConflict
	}
	if expectedVersion > 0 && (!exists || rec.version != expectedVersion) {
		return persistence.ErrConcurrencyConflict
	}
	r.records[id] = storedRecord[S]{
		state:   state,
		version: expectedVersion + 1,
	}
	return nil
}

func TestFromStateRepository_LifecycleAndOCC(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newSpyStateRepo()
	adapter := persistence.FromStateRepository(repo, counterDecider())

	errInvalid := errors.New("rejected")
	noopOrAdd := persistence.MustNewCommandHandler(adapter, addID, func(_ counterState, c addCmd) ([]any, error) {
		if c.delta < 0 {
			return []any{counterIncremented{delta: c.delta}}, errInvalid
		}
		if c.delta == 0 {
			return nil, nil
		}
		return []any{counterIncremented{delta: c.delta}}, nil
	})

	// Missing row starts at Initial(id), folds events, and saves with expectedVersion = 0.
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 7}); err != nil {
		t.Fatalf("first Handle failed: %v", err)
	}
	if repo.saveCalls != 1 || repo.lastExpected != 0 || repo.records["c-1"].version != 1 || repo.records["c-1"].state.count != 7 {
		t.Fatalf("after first Handle: calls=%d expected=%d rec=%+v", repo.saveCalls, repo.lastExpected, repo.records["c-1"])
	}

	// Second update loads version 1 and saves with expectedVersion = 1.
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 5}); err != nil {
		t.Fatalf("second Handle failed: %v", err)
	}
	if repo.saveCalls != 2 || repo.lastExpected != 1 || repo.records["c-1"].version != 2 || repo.records["c-1"].state.count != 12 {
		t.Fatalf("after second Handle: calls=%d expected=%d rec=%+v", repo.saveCalls, repo.lastExpected, repo.records["c-1"])
	}

	// Zero-event no-op skips Save.
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 0}); err != nil || repo.saveCalls != 2 {
		t.Fatalf("no-op err=%v saveCalls=%d", err, repo.saveCalls)
	}

	// Decide error skips Save even if non-empty events were also returned.
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: -1}); !errors.Is(err, errInvalid) || repo.saveCalls != 2 {
		t.Fatalf("decide error err=%v saveCalls=%d", err, repo.saveCalls)
	}

	// Non-OCC Save failure propagates cleanly.
	errDiskFull := errors.New("disk full")
	repo.saveErr = errDiskFull
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 2}); !errors.Is(err, errDiskFull) {
		t.Fatalf("saveErr = %v, want %v", err, errDiskFull)
	}
	repo.saveErr = nil

	// Negative version from buggy repo fails with ErrInvalidVersion.
	repo.forceNegVersion = true
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 1}); !errors.Is(err, persistence.ErrInvalidVersion) {
		t.Fatalf("negative version err = %v, want %v", err, persistence.ErrInvalidVersion)
	}
	repo.forceNegVersion = false

	// Non-ErrNotFound Load error stops before calling Save.
	callsBeforeLoadErr := repo.saveCalls
	dbDown := errors.New("db down")
	repo.loadErr = dbDown
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 1}); !errors.Is(err, dbDown) || repo.saveCalls != callsBeforeLoadErr {
		t.Fatalf("loadErr=%v saveCalls=%d", err, repo.saveCalls)
	}
}

func TestFromStateRepository_InitialIDAndMultiEventFolding(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newGenericStateRepo[trailState]()
	adapter := persistence.FromStateRepository(repo, trailDecider())

	var observedTrails []string
	handler := persistence.MustNewCommandHandler(
		adapter,
		func(c trailStepCmd) string { return c.id },
		func(state trailState, c trailStepCmd) ([]string, error) {
			if state.id != c.id {
				return nil, errors.New("Initial(id) was not propagated to decide")
			}
			observedTrails = append(observedTrails, state.trail)
			return c.steps, nil
		},
	)

	if err := handler.Handle(ctx, trailStepCmd{id: "ord-9", steps: []string{"created", "priced"}}); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	if err := handler.Handle(ctx, trailStepCmd{id: "ord-9", steps: []string{"confirmed"}}); err != nil {
		t.Fatalf("second Handle: %v", err)
	}

	wantObserved := []string{"init:ord-9", "init:ord-9>created>priced"}
	if !slices.Equal(observedTrails, wantObserved) {
		t.Fatalf("observedTrails = %v, want %v", observedTrails, wantObserved)
	}
	gotRec := repo.records["ord-9"]
	if gotRec.version != 2 || gotRec.state.trail != "init:ord-9>created>priced>confirmed" {
		t.Fatalf("stored record = %+v", gotRec)
	}
}

func TestFromStateRepository_CompetingWriterReloadsAndExhaustsRetries(t *testing.T) {
	t.Parallel()

	ctx := cqrs.WithTraceID(context.Background(), "trace-state-race")
	repo := newSpyStateRepo()

	var raced atomic.Bool
	repo.beforeSave = func(id string, expected int64) {
		if expected == 0 && raced.CompareAndSwap(false, true) {
			repo.mu.Lock()
			repo.records[id] = storedRecord[counterState]{
				state:   counterState{id: id, count: 100},
				version: 1,
			}
			repo.mu.Unlock()
		}
	}

	var seenCounts []int
	handler := persistence.MustNewCommandHandler(
		persistence.FromStateRepository(repo, counterDecider()),
		addID,
		func(state counterState, c addCmd) ([]any, error) {
			seenCounts = append(seenCounts, state.count)
			return decideAdd(state, c)
		},
	)

	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 2,
				RetryIf: func(err error) bool {
					return errors.Is(err, persistence.ErrConcurrencyConflict)
				},
			}),
		),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, handler)

	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "c-race", delta: 5}); dispatchErr != nil {
		t.Fatalf("Dispatch race retry failed: %v", dispatchErr)
	}
	if !slices.Equal(seenCounts, []int{0, 100}) {
		t.Fatalf("seenCounts = %v, want [0 100] (retry must reload competing write)", seenCounts)
	}
	if repo.lastExpected != 1 || repo.records["c-race"].version != 2 || repo.records["c-race"].state.count != 105 {
		t.Fatalf("after race retry: lastExpected=%d rec=%+v", repo.lastExpected, repo.records["c-race"])
	}

	// Exhausting MaxAttempts surfaces ErrConcurrencyConflict cleanly.
	repo.beforeSave = nil
	repo.failNextConflicts.Store(5)
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "c-race", delta: 1}); !errors.Is(dispatchErr, persistence.ErrConcurrencyConflict) {
		t.Fatalf("exhausted retries err = %v, want %v", dispatchErr, persistence.ErrConcurrencyConflict)
	}
}
