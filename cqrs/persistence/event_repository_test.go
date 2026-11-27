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

type spyEventRepo struct {
	mu                sync.RWMutex
	streams           map[string][]any
	loadErr           error
	appendErr         error
	forceNegVersion   bool
	appendCalls       int
	lastExpected      int64
	failNextConflicts atomic.Int32
}

func newSpyEventRepo() *spyEventRepo {
	return &spyEventRepo{streams: make(map[string][]any)}
}

func (r *spyEventRepo) Load(_ context.Context, id string) (events []any, version int64, err error) {
	if r.loadErr != nil {
		return nil, 0, r.loadErr
	}
	if r.forceNegVersion {
		return nil, -1, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	evs := r.streams[id]
	return append([]any(nil), evs...), int64(len(evs)), nil
}

func (r *spyEventRepo) Append(_ context.Context, id string, expectedVersion int64, events []any) error {
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
	r.streams[id] = append(current, events...)
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
			return nil, errRejected
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

	// First append uses expectedVersion = 0; zero-event no-op skips Append; second appends at 1.
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

	if err := handler.Handle(ctx, addCmd{id: "s-1", delta: 3}); err != nil {
		t.Fatalf("second Handle failed: %v", err)
	}
	if repo.lastExpected != 1 || repo.appendCalls != 3 || len(repo.streams["s-1"]) != 2 {
		t.Fatalf("after second Handle: lastExpected=%d appendCalls=%d len=%d", repo.lastExpected, repo.appendCalls, len(repo.streams["s-1"]))
	}
}

func TestFromEventRepository_RetryMiddlewareAndCommandBusIntegration(t *testing.T) {
	t.Parallel()

	repo := newSpyEventRepo()
	repo.failNextConflicts.Store(2)

	errInvalidDelta := errors.New("delta must be positive")
	handler := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, counterDecider()),
		addID,
		func(_ counterState, c addCmd) ([]any, error) {
			if c.delta <= 0 {
				return nil, errInvalidDelta
			}
			return []any{counterIncremented{delta: c.delta}}, nil
		},
	)

	var rejected []error
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
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errInvalidDelta) {
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
	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "ctr-occ", delta: 10}); dispatchErr != nil {
		t.Fatalf("Dispatch retry failed: %v", dispatchErr)
	}
	if repo.appendCalls != 3 {
		t.Fatalf("appendCalls = %d, want 3 (2 conflicts + 1 success)", repo.appendCalls)
	}

	if dispatchErr := bus.Dispatch(ctx, addCmd{id: "ctr-occ", delta: 0}); dispatchErr != nil {
		t.Fatalf("rejected Dispatch should be masked, got: %v", dispatchErr)
	}
	if len(rejected) != 1 || !errors.Is(rejected[0], errInvalidDelta) {
		t.Fatalf("rejected = %v, want [errInvalidDelta]", rejected)
	}

	events, ver, loadErr := repo.Load(ctx, "ctr-occ")
	if loadErr != nil || ver != 1 || len(events) != 1 {
		t.Fatalf("Load = (%v, %d, %v), want 1 event ver=1", events, ver, loadErr)
	}
}
