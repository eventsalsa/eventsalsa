package persistence_test

import (
	"context"
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/persistence"
)

type counterState struct {
	id    string
	count int
}

type counterIncremented struct{ delta int }

func counterDecider() persistence.Decider[counterState, any] {
	return persistence.Decider[counterState, any]{
		Initial: func(id string) counterState {
			return counterState{id: id}
		},
		Evolve: func(state counterState, ev any) counterState {
			if inc, ok := ev.(counterIncremented); ok {
				state.count += inc.delta
			}
			return state
		},
	}
}

type addCmd struct {
	id    string
	delta int
}

type spyRunner struct {
	calledID string
	retErr   error
}

func (s *spyRunner) Run(_ context.Context, id string, decide func(counterState) ([]any, error)) error {
	s.calledID = id
	if s.retErr != nil {
		return s.retErr
	}
	_, err := decide(counterState{id: id, count: 10})
	return err
}

func TestNewCommandHandler_ValidationAndExecution(t *testing.T) {
	t.Parallel()

	idOf := func(c addCmd) string { return c.id }
	decide := func(_ counterState, c addCmd) ([]any, error) {
		if c.delta <= 0 {
			return nil, errors.New("non-positive delta")
		}
		return []any{counterIncremented{delta: c.delta}}, nil
	}

	var typedNilRunner *persistence.StateRunner[counterState, any]
	for _, tc := range []struct {
		name    string
		runner  persistence.Runner[counterState, any]
		idOf    func(addCmd) string
		decide  func(counterState, addCmd) ([]any, error)
		wantErr error
	}{
		{name: "nil runner", runner: nil, idOf: idOf, decide: decide, wantErr: persistence.ErrNilRunner},
		{name: "typed-nil runner", runner: typedNilRunner, idOf: idOf, decide: decide, wantErr: persistence.ErrNilRunner},
		{name: "nil idOf", runner: &spyRunner{}, idOf: nil, decide: decide, wantErr: persistence.ErrNilTargetID},
		{name: "nil decide", runner: &spyRunner{}, idOf: idOf, decide: nil, wantErr: persistence.ErrNilDecide},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, err := persistence.NewCommandHandler(tc.runner, tc.idOf, tc.decide)
			if h != nil {
				t.Fatalf("expected nil handler, got %v", h)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}

	runner := &spyRunner{}
	handler := persistence.MustNewCommandHandler(runner, idOf, decide)

	if err := handler.Handle(nil, addCmd{id: "c-1", delta: 1}); !errors.Is(err, persistence.ErrNilContext) { //nolint:staticcheck // deliberate nil ctx test
		t.Fatalf("nil ctx err = %v, want %v", err, persistence.ErrNilContext)
	}
	if err := handler.Handle(context.Background(), addCmd{id: "", delta: 1}); !errors.Is(err, persistence.ErrEmptyTargetID) {
		t.Fatalf("empty id err = %v, want %v", err, persistence.ErrEmptyTargetID)
	}
	if err := handler.Handle(context.Background(), addCmd{id: "  \t ", delta: 1}); !errors.Is(err, persistence.ErrEmptyTargetID) {
		t.Fatalf("blank id err = %v, want %v", err, persistence.ErrEmptyTargetID)
	}

	if err := handler.Handle(context.Background(), addCmd{id: "ctr-1", delta: 5}); err != nil {
		t.Fatalf("Handle unexpected error: %v", err)
	}
	if runner.calledID != "ctr-1" {
		t.Fatalf("calledID = %q, want %q", runner.calledID, "ctr-1")
	}
}

func TestMustNewCommandHandler_PanicsOnInvalid(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from MustNewCommandHandler on invalid args")
		}
	}()
	persistence.MustNewCommandHandler[counterState, any, addCmd](nil, nil, nil)
}
