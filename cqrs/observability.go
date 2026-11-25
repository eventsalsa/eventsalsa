package cqrs

import "context"

type observabilityContextKey struct{}

// Observability carries distributed trace and causal identifiers across command and query handlers.
type Observability struct {
	CommandID   string
	TraceID     string
	CausationID string
}

// WithObservability returns a child context carrying obs. Callers must pass a non-nil ctx.
func WithObservability(ctx context.Context, obs Observability) context.Context {
	return context.WithValue(ctx, observabilityContextKey{}, obs)
}

// ObservabilityFromContext retrieves Observability metadata from ctx, if present.
func ObservabilityFromContext(ctx context.Context) (Observability, bool) {
	if ctx == nil {
		return Observability{}, false
	}
	obs, ok := ctx.Value(observabilityContextKey{}).(Observability)
	return obs, ok
}

// WithTraceID returns a child context with TraceID set while preserving CommandID and CausationID.
// Callers must pass a non-nil ctx.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	obs, _ := ObservabilityFromContext(ctx)
	obs.TraceID = traceID
	return WithObservability(ctx, obs)
}

// TraceIDFromContext returns the active TraceID from ctx, or empty string if unset.
func TraceIDFromContext(ctx context.Context) string {
	obs, ok := ObservabilityFromContext(ctx)
	if !ok {
		return ""
	}
	return obs.TraceID
}

// CommandIDFromContext returns the active CommandID from ctx, or empty string if unset.
func CommandIDFromContext(ctx context.Context) string {
	obs, ok := ObservabilityFromContext(ctx)
	if !ok {
		return ""
	}
	return obs.CommandID
}

// CausationIDFromContext returns the active CausationID from ctx, or empty string if unset.
func CausationIDFromContext(ctx context.Context) string {
	obs, ok := ObservabilityFromContext(ctx)
	if !ok {
		return ""
	}
	return obs.CausationID
}
