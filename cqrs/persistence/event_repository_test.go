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

func TestNewEventRunner_Validation(t *testing.T) {
	t.Parallel()

	d := counterDecider()
	var typedNilRepo *spyEventRepo

	for _, tc := range []struct {
		name    string
		repo    persistence.EventRepository[any]
		decider persistence.Decider[counterState, any]
		wantErr error
	}{
		{name: "nil repo", repo: nil, decider: d, wantErr: persistence.ErrNilRepository},
		{name: "typed-nil repo", repo: typedNilRepo, decider: d, wantErr: persistence.ErrNilRepository},
		{name: "nil Initial", repo: newSpyEventRepo(), decider: persistence.Decider[counterState, any]{Evolve: d.Evolve}, wantErr: persistence.ErrNilInitial},
		{name: "nil Evolve", repo: newSpyEventRepo(), decider: persistence.Decider[counterState, any]{Initial: d.Initial}, wantErr: persistence.ErrNilEvolve},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runner, err := persistence.NewEventRunner(tc.repo, tc.decider)
			if runner != nil {
				t.Fatalf("expected nil runner, got %+v", runner)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestEventRunner_LifecycleAndGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newSpyEventRepo()
	runner := persistence.MustNewEventRunner(repo, counterDecider())

	// Nil receiver and guard checks.
	var nilRunner *persistence.EventRunner[counterState, any]
	if _, _, err := nilRunner.Load(ctx, "s-1"); !errors.Is(err, persistence.ErrNilRunner) {
		t.Fatalf("nil Load err = %v, want %v", err, persistence.ErrNilRunner)
	}
	if err := nilRunner.Run(ctx, "s-1", func(counterState) ([]any, error) { return nil, nil }); !errors.Is(err, persistence.ErrNilRunner) {
		t.Fatalf("nil Run err = %v, want %v", err, persistence.ErrNilRunner)
	}
	if _, _, err := runner.Load(nil, "s-1"); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // deliberate nil ctx test
		t.Fatalf("nil ctx Load err = %v, want %v", err, persistence.ErrNilContext)
	}
	if _, _, err := runner.Load(ctx, "  "); !errors.Is(err, persistence.ErrEmptyTargetID) {
		t.Fatalf("blank id Load err = %v, want %v", err, persistence.ErrEmptyTargetID)
	}
	if err := runner.Run(ctx, "s-1", nil); !errors.Is(err, persistence.ErrNilDecide) {
		t.Fatalf("nil decide Run err = %v, want %v", err, persistence.ErrNilDecide)
	}

	// ErrNotFound from Load is normalized to Initial(id) at version 0.
	repo.loadErr = persistence.ErrNotFound
	state, version, err := runner.Load(ctx, "s-missing")
	if err != nil || version != 0 || state.id != "s-missing" || state.count != 0 {
		t.Fatalf("Load on ErrNotFound = (%+v, %d, %v)", state, version, err)
	}
	repo.loadErr = nil

	// Negative version from buggy repo fails with ErrInvalidVersion.
	repo.forceNegVersion = true
	if _, _, negErr := runner.Load(ctx, "s-neg"); !errors.Is(negErr, persistence.ErrInvalidVersion) {
		t.Fatalf("negative version err = %v, want %v", negErr, persistence.ErrInvalidVersion)
	}
	repo.forceNegVersion = false

	// First append uses expectedVersion = 0; zero-event decide skips Append; second appends at 1.
	if runErr := runner.Run(ctx, "s-1", func(s counterState) ([]any, error) {
		return []any{counterIncremented{delta: 5}}, nil
	}); runErr != nil {
		t.Fatalf("first Run failed: %v", runErr)
	}
	if repo.lastExpected != 0 || repo.appendCalls != 1 {
		t.Fatalf("after first Run: lastExpected=%d appendCalls=%d", repo.lastExpected, repo.appendCalls)
	}

	if noopErr := runner.Run(ctx, "s-1", func(s counterState) ([]any, error) {
		if s.count != 5 {
			t.Fatalf("reconstituted count = %d, want 5", s.count)
		}
		return nil, nil
	}); noopErr != nil || repo.appendCalls != 1 {
		t.Fatalf("no-op Run err=%v appendCalls=%d", noopErr, repo.appendCalls)
	}

	if secondErr := runner.Run(ctx, "s-1", func(s counterState) ([]any, error) {
		return []any{counterIncremented{delta: 3}}, nil
	}); secondErr != nil {
		t.Fatalf("second Run failed: %v", secondErr)
	}
	if repo.lastExpected != 1 || repo.appendCalls != 2 {
		t.Fatalf("after second Run: lastExpected=%d appendCalls=%d", repo.lastExpected, repo.appendCalls)
	}

	finalState, finalVer, loadErr := runner.Load(ctx, "s-1")
	if loadErr != nil || finalVer != 2 || finalState.count != 8 {
		t.Fatalf("final Load = (%+v, %d, %v), want count=8 ver=2", finalState, finalVer, loadErr)
	}
}

func TestEventRunner_RetryMiddlewareAndCommandBusIntegration(t *testing.T) {
	t.Parallel()

	repo := newSpyEventRepo()
	repo.failNextConflicts.Store(2)
	runner := persistence.MustNewEventRunner(repo, counterDecider())

	errInvalidDelta := errors.New("delta must be positive")
	handler := persistence.MustNewCommandHandler(
		runner,
		func(c addCmd) string { return c.id },
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

	state, ver, loadErr := runner.Load(ctx, "ctr-occ")
	if loadErr != nil || ver != 1 || state.count != 10 {
		t.Fatalf("Load = (%+v, %d, %v), want count=10 ver=1", state, ver, loadErr)
	}
}

func TestMustNewEventRunner_PanicsOnInvalid(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from MustNewEventRunner")
		}
	}()
	persistence.MustNewEventRunner[counterState, any](nil, counterDecider())
}
