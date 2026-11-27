package command_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
)

func TestObservabilityMiddleware(t *testing.T) {
	t.Parallel()

	type parentCmd struct{}
	type childCmd struct{}

	bus, err := command.NewBus(
		command.WithMiddleware(command.ObservabilityMiddleware()),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	var parentObs, childObs, siblingObs cqrs.Observability

	if regErr := command.Register(bus, command.Func[childCmd](func(ctx context.Context, _ childCmd) error {
		obs, ok := cqrs.ObservabilityFromContext(ctx)
		if !ok {
			return errors.New("missing observability on child")
		}
		if childObs.CommandID == "" {
			childObs = obs
		} else {
			siblingObs = obs
		}
		return nil
	})); regErr != nil {
		t.Fatalf("Register child: %v", regErr)
	}

	if regErr := command.Register(bus, command.Func[parentCmd](func(ctx context.Context, _ parentCmd) error {
		obs, ok := cqrs.ObservabilityFromContext(ctx)
		if !ok {
			return errors.New("missing observability on parent")
		}
		parentObs = obs
		if dErr := bus.Dispatch(ctx, childCmd{}); dErr != nil {
			return dErr
		}
		return bus.Dispatch(ctx, childCmd{})
	})); regErr != nil {
		t.Fatalf("Register parent: %v", regErr)
	}

	seedCtx := cqrs.WithTraceID(context.Background(), "incoming-trace")
	if dErr := bus.Dispatch(seedCtx, parentCmd{}); dErr != nil {
		t.Fatalf("Dispatch parent: %v", dErr)
	}

	if parentObs.TraceID != "incoming-trace" || parentObs.CommandID == "" || parentObs.CausationID != "" {
		t.Fatalf("unexpected parent observability: %+v", parentObs)
	}
	if childObs.TraceID != "incoming-trace" ||
		childObs.CausationID != parentObs.CommandID ||
		childObs.CommandID == "" ||
		childObs.CommandID == parentObs.CommandID {
		t.Fatalf("unexpected child observability: %+v (parent=%+v)", childObs, parentObs)
	}
	if siblingObs.CausationID != parentObs.CommandID || siblingObs.CommandID == childObs.CommandID {
		t.Fatalf("expected sibling child to keep parent as cause with fresh CommandID: %+v", siblingObs)
	}
}

func TestObservabilityMiddleware_CustomGeneratorAndFallback(t *testing.T) {
	t.Parallel()

	type ping struct{}

	var seq atomic.Int64
	mw := command.ObservabilityMiddleware(
		nil,
		command.WithIDGenerator(nil),
		command.WithIDGenerator(func() string {
			n := seq.Add(1)
			if n == 2 {
				return "" // empty return triggers safe UUID v4 fallback
			}
			return "id-gen-trace"
		}),
	)

	var got cqrs.Observability
	h, err := command.Wrap(command.Func[ping](func(ctx context.Context, _ ping) error {
		got, _ = cqrs.ObservabilityFromContext(ctx)
		return nil
	}), mw)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if handleErr := h.Handle(context.Background(), ping{}); handleErr != nil {
		t.Fatalf("Handle: %v", handleErr)
	}

	if got.TraceID != "id-gen-trace" {
		t.Fatalf("got TraceID %q, want id-gen-trace", got.TraceID)
	}
	if got.CommandID == "" {
		t.Fatal("expected UUID fallback when custom generator returned empty string")
	}
}
