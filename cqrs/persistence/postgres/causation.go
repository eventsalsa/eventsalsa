package postgres

import (
	"context"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/store"
)

// ContextWithEventCausation returns a child context carrying trace and causal identity
// from pe so downstream command dispatches (e.g. from eventsalsa.dev/projector handlers)
// link TraceID and CausationID to the triggering event without overwriting an outer trace.
//
// Any prior CommandID on ctx is cleared so command.ObservabilityMiddleware promotes
// pe.EventID directly into the downstream command's CausationID.
//
//nolint:gocritic // hugeParam: matches store.PersistedEvent value contract
func ContextWithEventCausation(ctx context.Context, pe store.PersistedEvent) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	existing, _ := cqrs.ObservabilityFromContext(ctx)

	traceID := existing.TraceID
	if pe.TraceID.Valid && pe.TraceID.String != "" {
		traceID = pe.TraceID.String
	}

	var causationID string
	if pe.EventID != uuid.Nil {
		causationID = pe.EventID.String()
	} else if pe.CausationID.Valid && pe.CausationID.String != "" {
		causationID = pe.CausationID.String
	}

	return cqrs.WithObservability(ctx, cqrs.Observability{
		TraceID:     traceID,
		CausationID: causationID,
	})
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
