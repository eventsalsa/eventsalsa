package cqrs

import (
	"context"

	"github.com/google/uuid"
)

type causationContextKey struct{}

// Causation carries execution causality and distributed tracing identifiers across handlers.
type Causation struct {
	CommandID   string
	TraceID     string
	CausationID string
}

// WithCausation returns a child context carrying c. Passing a nil ctx normalizes to context.Background.
func WithCausation(ctx context.Context, c Causation) context.Context {
	return context.WithValue(normalizeContext(ctx), causationContextKey{}, c)
}

// CausationFromContext retrieves Causation metadata from ctx, if present.
func CausationFromContext(ctx context.Context) (Causation, bool) {
	if ctx == nil {
		return Causation{}, false
	}
	c, ok := ctx.Value(causationContextKey{}).(Causation)
	return c, ok
}

// WithTraceID returns a child context with TraceID set while preserving CommandID and CausationID.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	c, _ := CausationFromContext(ctx)
	c.TraceID = traceID
	return WithCausation(ctx, c)
}

// TraceIDFromContext returns the active TraceID from ctx, or empty string if unset.
func TraceIDFromContext(ctx context.Context) string {
	c, ok := CausationFromContext(ctx)
	if !ok {
		return ""
	}
	return c.TraceID
}

// CommandIDFromContext returns the active CommandID from ctx, or empty string if unset.
func CommandIDFromContext(ctx context.Context) string {
	c, ok := CausationFromContext(ctx)
	if !ok {
		return ""
	}
	return c.CommandID
}

// CausationIDFromContext returns the active CausationID from ctx, or empty string if unset.
func CausationIDFromContext(ctx context.Context) string {
	c, ok := CausationFromContext(ctx)
	if !ok {
		return ""
	}
	return c.CausationID
}

type causationOptions struct {
	idGen func() string
}

// CausationOption configures CausationMiddleware.
type CausationOption func(*causationOptions)

// WithIDGenerator overrides the default uuid.NewString generator used by CausationMiddleware
// when minting CommandID and missing TraceID values. Nil functions are ignored.
func WithIDGenerator(fn func() string) CausationOption {
	return func(o *causationOptions) {
		if fn != nil {
			o.idGen = fn
		}
	}
}

// CausationMiddleware stamps a fresh CommandID onto ctx for each command execution, preserves or
// initializes TraceID, and promotes any parent ctx CommandID into CausationID.
//
// Mount CausationMiddleware OUTSIDE RetryCommandMiddleware so retries share the same CommandID.
func CausationMiddleware(opts ...CausationOption) CommandMiddleware {
	cfg := causationOptions{
		idGen: uuid.NewString,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	nextID := func() string {
		if cfg.idGen != nil {
			if id := cfg.idGen(); id != "" {
				return id
			}
		}
		return uuid.NewString()
	}

	return func(next UntypedCommandFunc) UntypedCommandFunc {
		return func(ctx context.Context, cmd any) error {
			existing, _ := CausationFromContext(ctx)

			causationID := existing.CausationID
			if existing.CommandID != "" {
				causationID = existing.CommandID
			}
			traceID := existing.TraceID
			if traceID == "" {
				traceID = nextID()
			}

			childCtx := WithCausation(ctx, Causation{
				CommandID:   nextID(),
				TraceID:     traceID,
				CausationID: causationID,
			})
			return next(childCtx, cmd)
		}
	}
}
