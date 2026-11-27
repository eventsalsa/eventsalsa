package persistence_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
)

type storedCounter struct {
	state   counterState
	version int64
}

type spyStateRepo struct {
	mu                sync.RWMutex
	records           map[string]storedCounter
	loadErr           error
	saveErr           error
	forceNegVersion   bool
	saveCalls         int
	lastExpected      int64
	failNextConflicts atomic.Int32
}

func newSpyStateRepo() *spyStateRepo {
	return &spyStateRepo{records: make(map[string]storedCounter)}
}

func (r *spyStateRepo) Load(_ context.Context, id string) (state counterState, version int64, err error) {
	if r.loadErr != nil {
		return counterState{}, 0, r.loadErr
	}
	if r.forceNegVersion {
		return counterState{id: id}, -1, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[id]
	if !ok {
		return counterState{}, 0, persistence.ErrNotFound
	}
	return rec.state, rec.version, nil
}

func (r *spyStateRepo) Save(_ context.Context, id string, expectedVersion int64, state counterState) error {
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
	r.records[id] = storedCounter{
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
			return nil, errInvalid
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

	// Decide error skips Save.
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: -1}); !errors.Is(err, errInvalid) || repo.saveCalls != 2 {
		t.Fatalf("decide error err=%v saveCalls=%d", err, repo.saveCalls)
	}

	// Negative version from buggy repo fails with ErrInvalidVersion.
	repo.forceNegVersion = true
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 1}); !errors.Is(err, persistence.ErrInvalidVersion) {
		t.Fatalf("negative version err = %v, want %v", err, persistence.ErrInvalidVersion)
	}
	repo.forceNegVersion = false

	// Non-ErrNotFound Load error stops before calling Save.
	dbDown := errors.New("db down")
	repo.loadErr = dbDown
	if err := noopOrAdd.Handle(ctx, addCmd{id: "c-1", delta: 1}); !errors.Is(err, dbDown) || repo.saveCalls != 2 {
		t.Fatalf("loadErr=%v saveCalls=%d", err, repo.saveCalls)
	}
}

func TestFromStateRepository_OCCRetryMiddlewareIntegration(t *testing.T) {
	t.Parallel()

	repo := newSpyStateRepo()
	repo.failNextConflicts.Store(2)

	handler := persistence.MustNewCommandHandler(
		persistence.FromStateRepository(repo, counterDecider()),
		addID,
		decideAdd,
	)
	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
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

	ctx := cqrs.WithTraceID(context.Background(), "trace-state-occ")
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "c-occ", delta: 9}); dispatchErr != nil {
		t.Fatalf("Dispatch retry failed: %v", dispatchErr)
	}
	if repo.saveCalls != 3 || repo.records["c-occ"].version != 1 || repo.records["c-occ"].state.count != 9 {
		t.Fatalf("calls=%d rec=%+v", repo.saveCalls, repo.records["c-occ"])
	}
}
