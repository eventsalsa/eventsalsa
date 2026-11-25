package query_test

import (
	"context"
	"sync/atomic"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/query"
)

func TestObservabilityMiddleware(t *testing.T) {
	t.Parallel()

	type lookupQ struct{}

	t.Run("mints TraceID when absent and preserves existing causal fields", func(t *testing.T) {
		t.Parallel()

		var seq atomic.Int64
		bus, err := query.NewBus(
			query.WithMiddleware(query.ObservabilityMiddleware(
				nil,
				query.WithIDGenerator(nil),
				query.WithIDGenerator(func() string {
					if seq.Add(1) == 1 {
						return "" // empty return falls back to UUID v4
					}
					return "custom-trace"
				}),
			)),
		)
		if err != nil {
			t.Fatalf("NewBus: %v", err)
		}

		query.MustRegister(bus, query.Func[lookupQ, cqrs.Observability](
			func(ctx context.Context, _ lookupQ) (cqrs.Observability, error) {
				obs, _ := cqrs.ObservabilityFromContext(ctx)
				return obs, nil
			},
		))

		// Absent TraceID -> fallback UUID minted without fabricating CommandID
		got, execErr := query.Execute[cqrs.Observability](context.Background(), bus, lookupQ{})
		if execErr != nil {
			t.Fatalf("Execute: %v", execErr)
		}
		if got.TraceID == "" || got.CommandID != "" || got.CausationID != "" {
			t.Fatalf("unexpected minted observability: %+v", got)
		}

		// Custom generator invoked when TraceID is unset
		gotCustom, execErr := query.Execute[cqrs.Observability](context.Background(), bus, lookupQ{})
		if execErr != nil {
			t.Fatalf("Execute custom: %v", execErr)
		}
		if gotCustom.TraceID != "custom-trace" {
			t.Fatalf("got TraceID %q, want custom-trace", gotCustom.TraceID)
		}

		// Existing TraceID, CommandID, and CausationID are preserved untouched
		seedCtx := cqrs.WithObservability(context.Background(), cqrs.Observability{
			TraceID:     "incoming-trace",
			CommandID:   "cmd-9",
			CausationID: "cmd-8",
		})
		preserved, execErr := query.Execute[cqrs.Observability](seedCtx, bus, lookupQ{})
		if execErr != nil {
			t.Fatalf("Execute preserved: %v", execErr)
		}
		if preserved.TraceID != "incoming-trace" || preserved.CommandID != "cmd-9" || preserved.CausationID != "cmd-8" {
			t.Fatalf("existing observability mutated: %+v", preserved)
		}
	})
}
