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

func addID(c addCmd) string { return c.id }

func decideAdd(_ counterState, c addCmd) ([]any, error) {
	if c.delta <= 0 {
		return nil, errors.New("delta must be positive")
	}
	return []any{counterIncremented{delta: c.delta}}, nil
}

type trailState struct {
	id    string
	trail string
}

type trailStepCmd struct {
	id    string
	steps []string
}

func trailDecider() persistence.Decider[trailState, string] {
	return persistence.Decider[trailState, string]{
		Initial: func(id string) trailState {
			return trailState{id: id, trail: "init:" + id}
		},
		Evolve: func(state trailState, ev string) trailState {
			state.trail += ">" + ev
			return state
		},
	}
}

func TestNewCommandHandler_Validation(t *testing.T) {
	t.Parallel()

	d := counterDecider()
	var (
		typedNilStateRepo *spyStateRepo
		typedNilEventRepo *spyEventRepo
	)

	stateAdapter := persistence.FromStateRepository(newSpyStateRepo(), d)
	eventAdapter := persistence.FromEventRepository(newSpyEventRepo(), d)

	for _, tc := range []struct {
		name    string
		adapter persistence.Adapter[counterState, any]
		idOf    func(addCmd) string
		decide  func(counterState, addCmd) ([]any, error)
		wantErr error
	}{
		{name: "zero adapter", adapter: persistence.Adapter[counterState, any]{}, idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilAdapter},
		{name: "nil state repo", adapter: persistence.FromStateRepository[counterState, any](nil, d), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilRepository},
		{name: "typed-nil state repo", adapter: persistence.FromStateRepository(typedNilStateRepo, d), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilRepository},
		{name: "nil event repo", adapter: persistence.FromEventRepository[counterState, any](nil, d), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilRepository},
		{name: "typed-nil event repo", adapter: persistence.FromEventRepository(typedNilEventRepo, d), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilRepository},
		{name: "state nil Initial", adapter: persistence.FromStateRepository(newSpyStateRepo(), persistence.Decider[counterState, any]{Evolve: d.Evolve}), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilInitial},
		{name: "state nil Evolve", adapter: persistence.FromStateRepository(newSpyStateRepo(), persistence.Decider[counterState, any]{Initial: d.Initial}), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilEvolve},
		{name: "event nil Initial", adapter: persistence.FromEventRepository(newSpyEventRepo(), persistence.Decider[counterState, any]{Evolve: d.Evolve}), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilInitial},
		{name: "event nil Evolve", adapter: persistence.FromEventRepository(newSpyEventRepo(), persistence.Decider[counterState, any]{Initial: d.Initial}), idOf: addID, decide: decideAdd, wantErr: persistence.ErrNilEvolve},
		{name: "state nil idOf", adapter: stateAdapter, idOf: nil, decide: decideAdd, wantErr: persistence.ErrNilTargetID},
		{name: "event nil idOf", adapter: eventAdapter, idOf: nil, decide: decideAdd, wantErr: persistence.ErrNilTargetID},
		{name: "state nil decide", adapter: stateAdapter, idOf: addID, decide: nil, wantErr: persistence.ErrNilDecide},
		{name: "event nil decide", adapter: eventAdapter, idOf: addID, decide: nil, wantErr: persistence.ErrNilDecide},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler, err := persistence.NewCommandHandler(tc.adapter, tc.idOf, tc.decide)
			if handler != nil {
				t.Fatalf("expected nil handler, got %v", handler)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestMustNewCommandHandler_PanicsOnInvalid(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from MustNewCommandHandler")
		}
	}()
	persistence.MustNewCommandHandler(persistence.Adapter[counterState, any]{}, addID, decideAdd)
}

func TestCommandHandler_InputGuards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d := counterDecider()

	stateRepo := newSpyStateRepo()
	eventRepo := newSpyEventRepo()
	stateH := persistence.MustNewCommandHandler(persistence.FromStateRepository(stateRepo, d), addID, decideAdd)
	eventH := persistence.MustNewCommandHandler(persistence.FromEventRepository(eventRepo, d), addID, decideAdd)

	for _, tc := range []struct {
		name    string
		ctx     context.Context //nolint:containedctx // table-driven guard cases
		cmd     addCmd
		wantErr error
	}{
		{name: "nil context", ctx: nil, cmd: addCmd{id: "c-1", delta: 1}, wantErr: persistence.ErrNilContext},
		{name: "empty id", ctx: ctx, cmd: addCmd{id: "", delta: 1}, wantErr: persistence.ErrEmptyTargetID},
		{name: "blank id", ctx: ctx, cmd: addCmd{id: "  \t ", delta: 1}, wantErr: persistence.ErrEmptyTargetID},
	} {
		if err := stateH.Handle(tc.ctx, tc.cmd); !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s stateH err = %v, want %v", tc.name, err, tc.wantErr)
		}
		if err := eventH.Handle(tc.ctx, tc.cmd); !errors.Is(err, tc.wantErr) {
			t.Fatalf("%s eventH err = %v, want %v", tc.name, err, tc.wantErr)
		}
	}

	if stateRepo.loadCalls != 0 || stateRepo.saveCalls != 0 {
		t.Fatalf("stateRepo should not be touched on invalid inputs, got load=%d save=%d", stateRepo.loadCalls, stateRepo.saveCalls)
	}
	if eventRepo.loadCalls != 0 || eventRepo.appendCalls != 0 {
		t.Fatalf("eventRepo should not be touched on invalid inputs, got load=%d append=%d", eventRepo.loadCalls, eventRepo.appendCalls)
	}
}
