package cqrs_test

import (
	"context"
	"testing"

	"eventsalsa.dev/cqrs"
)

func TestObservabilityContextHelpers(t *testing.T) {
	t.Parallel()

	if obs, ok := cqrs.ObservabilityFromContext(context.Background()); ok || obs != (cqrs.Observability{}) {
		t.Fatalf("expected empty observability from fresh context, got %+v ok=%v", obs, ok)
	}
	if got := cqrs.TraceIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty trace ID, got %q", got)
	}
	if got := cqrs.CommandIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty command ID, got %q", got)
	}
	if got := cqrs.CausationIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty causation ID, got %q", got)
	}

	// Pure read-only getters must return empty when passed a nil context.
	//nolint:staticcheck // SA1012: intentionally verifying read-only nil guard
	if obs, ok := cqrs.ObservabilityFromContext(nil); ok || obs != (cqrs.Observability{}) {
		t.Fatalf("expected empty observability from nil context, got %+v ok=%v", obs, ok)
	}

	ctx := cqrs.WithObservability(context.Background(), cqrs.Observability{
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

func TestWithObservability_PanicsOnNilContext(t *testing.T) {
	t.Parallel()

	t.Run("WithObservability rejects nil ctx loudly", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recover() == nil {
				t.Fatal("expected WithObservability(nil, ...) to panic")
			}
		}()
		//nolint:staticcheck // SA1012: verifying strict non-nil context contract
		_ = cqrs.WithObservability(nil, cqrs.Observability{TraceID: "trace-1"})
	})

	t.Run("WithTraceID rejects nil ctx loudly", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recover() == nil {
				t.Fatal("expected WithTraceID(nil, ...) to panic")
			}
		}()
		//nolint:staticcheck // SA1012: verifying strict non-nil context contract
		_ = cqrs.WithTraceID(nil, "trace-1")
	})
}
