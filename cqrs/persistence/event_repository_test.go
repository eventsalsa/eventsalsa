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

type genericEventRepo[E any] struct {
	mu                sync.RWMutex
	streams           map[string][]E
	loadErr           error
	appendErr         error
	beforeAppend      func(id string, expected int64)
	forceNegVersion   bool
	loadCalls         int
	appendCalls       int
	lastExpected      int64
	failNextConflicts atomic.Int32
}

type spyEventRepo = genericEventRepo[any]

func newGenericEventRepo[E any]() *genericEventRepo[E] {
	return &genericEventRepo[E]{streams: make(map[string][]E)}
}

func newSpyEventRepo() *spyEventRepo {
	return newGenericEventRepo[any]()
}

func (r *genericEventRepo[E]) Load(_ context.Context, id string) (events []E, version int64, err error) {
	r.mu.Lock()
	r.loadCalls++
	loadErr := r.loadErr
	forceNeg := r.forceNegVersion
	evs := slices.Clone(r.streams[id])
	r.mu.Unlock()

	if loadErr != nil {
		return nil, 0, loadErr
	}
	if forceNeg {
		return nil, -1, nil
	}
	return evs, int64(len(evs)), nil
}

func (r *genericEventRepo[E]) Append(_ context.Context, id string, expectedVersion int64, events []E) error {
	r.mu.Lock()
	beforeAppend := r.beforeAppend
	r.mu.Unlock()
	if beforeAppend != nil {
		beforeAppend(id, expectedVersion)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.appendCalls++
	r.lastExpected = expectedVersion

	if r.failNextConflicts.Load() > 0 {
		r.failNextConflicts.Add(-1)
		return persistence.ErrConcurrencyConflict
	}
	if r.appendErr != nil {
		return r.appendErr
	}
	current := r.streams[id]
	if int64(len(current)) != expectedVersion {
		return persistence.ErrConcurrencyConflict
	}
	r.streams[id] = append(slices.Clone(current), events...)
	return nil
}

func TestFromEventRepository_LifecycleAndGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newSpyEventRepo()
	adapter := persistence.FromEventRepository(repo, counterDecider())

	errRejected := errors.New("rejected")
	handler := persistence.MustNewCommandHandler(adapter, addID, func(_ counterState, c addCmd) ([]any, error) {
		if c.delta < 0 {
			return []any{counterIncremented{delta: c.delta}}, errRejected
		}
		if c.delta == 0 {
			return nil, nil
		}
		return []any{counterIncremented{delta: c.delta}}, nil
	})

	// ErrNotFound from Load is normalized to Initial(id) at expectedVersion = 0.
	repo.loadErr = persistence.ErrNotFound
	if err := handler.Handle(ctx, addCmd{id: "s-missing", delta: 4}); err != nil {
		t.Fatalf("ErrNotFound Load failed: %v", err)
	}
	if repo.lastExpected != 0 || repo.appendCalls != 1 {
		t.Fatalf("lastExpected=%d appendCalls=%d", repo.lastExpected, repo.appendCalls)
	}
	repo.loadErr = nil

	// Non-ErrNotFound Load error stops before decide or Append.
	dbErr := errors.New("store unavailable")
	repo.loadErr = dbErr
	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 1}); !errors.Is(err, dbErr) || repo.appendCalls != 1 {
		t.Fatalf("loadErr=%v appendCalls=%d", err, repo.appendCalls)
	}
	repo.loadErr = nil

	// Negative version from buggy repo fails with ErrInvalidVersion.
	repo.forceNegVersion = true
	if err := handler.Handle(ctx, addCmd{id: "s-neg", delta: 1}); !errors.Is(err, persistence.ErrInvalidVersion) {
		t.Fatalf("negative version err = %v, want %v", err, persistence.ErrInvalidVersion)
	}
	repo.forceNegVersion = false

	// First append uses expectedVersion = 0; zero-event and error decide results skip Append.
	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 5}); err != nil {
		t.Fatalf("first Handle failed: %v", err)
	}
	if repo.lastExpected != 0 || repo.appendCalls != 2 {
		t.Fatalf("after first Handle: lastExpected=%d appendCalls=%d", repo.lastExpected, repo.appendCalls)
	}

	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 0}); err != nil || repo.appendCalls != 2 {
		t.Fatalf("no-op err=%v appendCalls=%d", err, repo.appendCalls)
	}

	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: -1}); !errors.Is(err, errRejected) || repo.appendCalls != 2 {
		t.Fatalf("decide err=%v appendCalls=%d", err, repo.appendCalls)
	}

	// Non-OCC Append failure propagates cleanly.
	errAppendIO := errors.New("append I/O error")
	repo.appendErr = errAppendIO
	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 2}); !errors.Is(err, errAppendIO) {
		t.Fatalf("appendErr = %v, want %v", err, errAppendIO)
	}
	repo.appendErr = nil

	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 3}); err != nil {
		t.Fatalf("second Handle failed: %v", err)
	}
	if repo.lastExpected != 1 || repo.appendCalls != 4 || len(repo.streams["s-1"]) != 2 {
		t.Fatalf("after second Handle: lastExpected=%d appendCalls=%d len=%d", repo.lastExpected, repo.appendCalls, len(repo.streams["s-1"]))
	}
}

func TestFromEventRepository_ReconstitutesOrderedHistoryBeforeDecide(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newGenericEventRepo[string]()
	repo.streams["ord-seq"] = []string{"placed", "paid"}

	var observedTrail string
	handler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, trailDecider()),
		func(c trailStepCmd) string { return c.id },
		func(state trailState, c trailStepCmd) ([]string, error) {
			if state.id != c.id {
				return nil, errors.New("Initial(id) was not propagated to decide")
			}
			observedTrail = state.trail
			return c.steps, nil
		},
	)

	if err := handler.Handle(ctx, trailStepCmd{id: "ord-seq", steps: []string{"packed", "shipped"}}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if observedTrail != "init:ord-seq>placed>paid" {
		t.Fatalf("observedTrail = %q, want %q", observedTrail, "init:ord-seq>placed>paid")
	}
	wantStream := []string{"placed", "paid", "packed", "shipped"}
	if !slices.Equal(repo.streams["ord-seq"], wantStream) || repo.lastExpected != 2 {
		t.Fatalf("stream=%v expected=%d, want %v expected=2", repo.streams["ord-seq"], repo.lastExpected, wantStream)
	}
}

func TestFromEventRepository_CompetingWriterReloadsAndClassifies(t *testing.T) {
	t.Parallel()

	repo := newSpyEventRepo()
	var raced atomic.Bool
	repo.beforeAppend = func(id string, expected int64) {
		if id == "ctr-race" && expected == 0 && raced.CompareAndSwap(false, true) {
			repo.mu.Lock()
			repo.streams[id] = []any{counterIncremented{delta: 100}}
			repo.mu.Unlock()
		}
	}

	errAlreadyCapped := errors.New("counter capped")
	var seenCounts []int
	handler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, counterDecider()),
		addID,
		func(state counterState, c addCmd) ([]any, error) {
			seenCounts = append(seenCounts, state.count)
			if state.count >= 100 && c.delta > 50 {
				return nil, errAlreadyCapped
			}
			return decideAdd(state, c)
		},
	)

	var rejected []error
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
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errAlreadyCapped) {
				return command.Reject(err)
			}
			return err
		}),
		command.WithOnRejected(func(_ context.Context, _ any, rejErr error) {
			rejected = append(rejected, rejErr)
		}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, handler)

	ctx := cqrs.WithTraceID(context.Background(), "trace-occ")
	// Competing commit advances ctr-race to version 1; retry reloads count=100 and appends +10.
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "ctr-race", delta: 10}); dispatchErr != nil {
		t.Fatalf("Dispatch race retry failed: %v", dispatchErr)
	}
	if !slices.Equal(seenCounts, []int{0, 100}) {
		t.Fatalf("seenCounts = %v, want [0 100]", seenCounts)
	}
	events, ver, loadErr := repo.Load(ctx, "ctr-race")
	if loadErr != nil || ver != 2 || len(events) != 2 {
		t.Fatalf("Load = (%v, %d, %v), want 2 events ver=2", events, ver, loadErr)
	}

	// Competing commit on a fresh stream violates domain invariant upon retry and is rejected cleanly.
	raced.Store(false)
	seenCounts = nil
	repo.beforeAppend = func(id string, expected int64) {
		if id == "ctr-cap" && expected == 0 && raced.CompareAndSwap(false, true) {
			repo.mu.Lock()
			repo.streams[id] = []any{counterIncremented{delta: 100}}
			repo.mu.Unlock()
		}
	}
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "ctr-cap", delta: 60}); dispatchErr != nil {
		t.Fatalf("rejected Dispatch after reload should be masked, got: %v", dispatchErr)
	}
	if len(rejected) != 1 || !errors.Is(rejected[0], errAlreadyCapped) {
		t.Fatalf("rejected = %v, want [errAlreadyCapped]", rejected)
	}

	// Exhausting MaxAttempts returns ErrConcurrencyConflict untouched.
	repo.beforeAppend = nil
	repo.failNextConflicts.Store(5)
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "ctr-race", delta: 1}); !errors.Is(dispatchErr, persistence.ErrConcurrencyConflict) {
		t.Fatalf("exhausted retries err = %v, want %v", dispatchErr, persistence.ErrConcurrencyConflict)
	}
}
