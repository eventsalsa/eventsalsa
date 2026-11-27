package persistence_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/persistence"
)

type spyStateRepo struct {
	states    map[string]counterState
	loadErr   error
	saveErr   error
	saveCalls int
}

func newSpyStateRepo() *spyStateRepo {
	return &spyStateRepo{states: make(map[string]counterState)}
}

func (r *spyStateRepo) Load(_ context.Context, id string) (counterState, error) {
	if r.loadErr != nil {
		return counterState{}, r.loadErr
	}
	state, ok := r.states[id]
	if !ok {
		return counterState{}, persistence.ErrNotFound
	}
	return state, nil
}

func (r *spyStateRepo) Save(_ context.Context, state counterState) error {
	r.saveCalls++
	if r.saveErr != nil {
		return r.saveErr
	}
	r.states[state.id] = state
	return nil
}

func TestNewStateRunner_Validation(t *testing.T) {
	t.Parallel()

	d := counterDecider()
	var typedNilRepo *spyStateRepo

	for _, tc := range []struct {
		name    string
		repo    persistence.StateRepository[counterState]
		decider persistence.Decider[counterState, any]
		wantErr error
	}{
		{name: "nil repo", repo: nil, decider: d, wantErr: persistence.ErrNilRepository},
		{name: "typed-nil repo", repo: typedNilRepo, decider: d, wantErr: persistence.ErrNilRepository},
		{name: "nil Initial", repo: newSpyStateRepo(), decider: persistence.Decider[counterState, any]{Evolve: d.Evolve}, wantErr: persistence.ErrNilInitial},
		{name: "nil Evolve", repo: newSpyStateRepo(), decider: persistence.Decider[counterState, any]{Initial: d.Initial}, wantErr: persistence.ErrNilEvolve},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runner, err := persistence.NewStateRunner(tc.repo, tc.decider)
			if runner != nil {
				t.Fatalf("expected nil runner, got %+v", runner)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestStateRunner_LifecycleAndGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newSpyStateRepo()
	runner := persistence.MustNewStateRunner(repo, counterDecider())

	// Guard checks on Run and nil receiver.
	var nilRunner *persistence.StateRunner[counterState, any]
	if err := nilRunner.Run(ctx, "c-1", func(s counterState) ([]any, error) { return nil, nil }); !errors.Is(err, persistence.ErrNilRunner) {
		t.Fatalf("nil receiver err = %v, want %v", err, persistence.ErrNilRunner)
	}
	if err := runner.Run(nil, "c-1", func(s counterState) ([]any, error) { return nil, nil }); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // deliberate nil ctx test
		t.Fatalf("nil ctx err = %v, want %v", err, persistence.ErrNilContext)
	}
	if err := runner.Run(ctx, " ", func(s counterState) ([]any, error) { return nil, nil }); !errors.Is(err, persistence.ErrEmptyTargetID) {
		t.Fatalf("blank id err = %v, want %v", err, persistence.ErrEmptyTargetID)
	}
	if err := runner.Run(ctx, "c-1", nil); !errors.Is(err, persistence.ErrNilDecide) {
		t.Fatalf("nil decide err = %v, want %v", err, persistence.ErrNilDecide)
	}

	// Missing row starts from Initial(id), folds events, and calls Save once.
	err := runner.Run(ctx, "c-1", func(s counterState) ([]any, error) {
		if s.id != "c-1" || s.count != 0 {
			t.Fatalf("unexpected initial state: %+v", s)
		}
		return []any{counterIncremented{delta: 3}, counterIncremented{delta: 4}}, nil
	})
	if err != nil {
		t.Fatalf("first Run failed: %v", err)
	}
	if repo.saveCalls != 1 || repo.states["c-1"].count != 7 {
		t.Fatalf("after first Run: saveCalls=%d state=%+v", repo.saveCalls, repo.states["c-1"])
	}

	// Zero-event no-op skips Save completely.
	err = runner.Run(ctx, "c-1", func(s counterState) ([]any, error) {
		if s.count != 7 {
			t.Fatalf("loaded count = %d, want 7", s.count)
		}
		return nil, nil
	})
	if err != nil || repo.saveCalls != 1 {
		t.Fatalf("no-op Run err=%v saveCalls=%d, want nil and 1", err, repo.saveCalls)
	}

	// Decide error skips Save cleanly.
	boom := errors.New("rejected")
	err = runner.Run(ctx, "c-1", func(counterState) ([]any, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) || repo.saveCalls != 1 {
		t.Fatalf("decide error err=%v saveCalls=%d", err, repo.saveCalls)
	}

	// Non-ErrNotFound Load error stops before calling decide.
	dbDown := errors.New("db unavailable")
	repo.loadErr = dbDown
	decideCalled := false
	err = runner.Run(ctx, "c-1", func(counterState) ([]any, error) {
		decideCalled = true
		return nil, nil
	})
	if !errors.Is(err, dbDown) || decideCalled {
		t.Fatalf("loadErr err=%v decideCalled=%v", err, decideCalled)
	}
}

func TestMustNewStateRunner_PanicsOnInvalid(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from MustNewStateRunner")
		}
	}()
	persistence.MustNewStateRunner[counterState, any](nil, counterDecider())
}
