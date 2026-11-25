package cqrs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
)

func TestCausationContextHelpers(t *testing.T) {
	t.Parallel()

	// Nil context and empty lookups fail safely without panics.
	//nolint:staticcheck // SA1012: intentionally verifying nil context handling
	if c, ok := cqrs.CausationFromContext(nil); ok || c != (cqrs.Causation{}) {
		t.Fatalf("expected empty causation from nil context, got %+v ok=%v", c, ok)
	}
	//nolint:staticcheck // SA1012: intentionally verifying nil context getters
	if got := cqrs.TraceIDFromContext(nil); got != "" {
		t.Fatalf("expected empty trace ID, got %q", got)
	}
	//nolint:staticcheck // SA1012: intentionally verifying nil context getters
	if got := cqrs.CommandIDFromContext(nil); got != "" {
		t.Fatalf("expected empty command ID, got %q", got)
	}
	//nolint:staticcheck // SA1012: intentionally verifying nil context getters
	if got := cqrs.CausationIDFromContext(nil); got != "" {
		t.Fatalf("expected empty causation ID, got %q", got)
	}

	//nolint:staticcheck // SA1012: verifying WithCausation normalizes nil ctx
	ctx := cqrs.WithCausation(nil, cqrs.Causation{
		CommandID:   "cmd-1",
		TraceID:     "trace-1",
		CausationID: "cause-0",
	})
	if cqrs.CommandIDFromContext(ctx) != "cmd-1" ||
		cqrs.TraceIDFromContext(ctx) != "trace-1" ||
		cqrs.CausationIDFromContext(ctx) != "cause-0" {
		t.Fatalf("unexpected values stored on context: %+v", ctx)
	}

	// WithTraceID updates TraceID while preserving existing CommandID and CausationID.
	updated := cqrs.WithTraceID(ctx, "trace-2")
	if cqrs.TraceIDFromContext(updated) != "trace-2" ||
		cqrs.CommandIDFromContext(updated) != "cmd-1" ||
		cqrs.CausationIDFromContext(updated) != "cause-0" {
		t.Fatalf("WithTraceID failed to preserve sibling fields")
	}
}

func TestCausationMiddleware(t *testing.T) {
	t.Parallel()

	type parentCmd struct{}
	type childCmd struct{}

	bus, err := cqrs.NewCommandBus(
		cqrs.WithCommandMiddleware(cqrs.CausationMiddleware()),
	)
	if err != nil {
		t.Fatalf("NewCommandBus: %v", err)
	}

	var parentCausation, childCausation, siblingCausation cqrs.Causation

	if regErr := cqrs.RegisterCommand(bus, cqrs.CommandFunc[childCmd](func(ctx context.Context, _ childCmd) error {
		c, ok := cqrs.CausationFromContext(ctx)
		if !ok {
			return errors.New("missing causation on child")
		}
		if childCausation.CommandID == "" {
			childCausation = c
		} else {
			siblingCausation = c
		}
		return nil
	})); regErr != nil {
		t.Fatalf("RegisterCommand child: %v", regErr)
	}

	if regErr := cqrs.RegisterCommand(bus, cqrs.CommandFunc[parentCmd](func(ctx context.Context, _ parentCmd) error {
		c, ok := cqrs.CausationFromContext(ctx)
		if !ok {
			return errors.New("missing causation on parent")
		}
		parentCausation = c
		if dErr := bus.Dispatch(ctx, childCmd{}); dErr != nil {
			return dErr
		}
		return bus.Dispatch(ctx, childCmd{})
	})); regErr != nil {
		t.Fatalf("RegisterCommand parent: %v", regErr)
	}

	seedCtx := cqrs.WithTraceID(context.Background(), "incoming-trace")
	if dErr := bus.Dispatch(seedCtx, parentCmd{}); dErr != nil {
		t.Fatalf("Dispatch parent: %v", dErr)
	}

	if parentCausation.TraceID != "incoming-trace" || parentCausation.CommandID == "" || parentCausation.CausationID != "" {
		t.Fatalf("unexpected parent causation: %+v", parentCausation)
	}
	if childCausation.TraceID != "incoming-trace" ||
		childCausation.CausationID != parentCausation.CommandID ||
		childCausation.CommandID == "" ||
		childCausation.CommandID == parentCausation.CommandID {
		t.Fatalf("unexpected child causation: %+v (parent=%+v)", childCausation, parentCausation)
	}
	if siblingCausation.CausationID != parentCausation.CommandID || siblingCausation.CommandID == childCausation.CommandID {
		t.Fatalf("expected sibling child to keep parent as cause with fresh CommandID: %+v", siblingCausation)
	}
}

func TestCausationMiddlewareCustomGeneratorAndFallback(t *testing.T) {
	t.Parallel()

	type ping struct{}

	var seq atomic.Int64
	mw := cqrs.CausationMiddleware(
		nil,
		cqrs.WithIDGenerator(nil),
		cqrs.WithIDGenerator(func() string {
			n := seq.Add(1)
			if n == 2 {
				return "" // empty return triggers safe UUID v4 fallback
			}
			return "id-gen-trace"
		}),
	)

	var got cqrs.Causation
	h, err := cqrs.WrapCommand(cqrs.CommandFunc[ping](func(ctx context.Context, _ ping) error {
		got, _ = cqrs.CausationFromContext(ctx)
		return nil
	}), mw)
	if err != nil {
		t.Fatalf("WrapCommand: %v", err)
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
