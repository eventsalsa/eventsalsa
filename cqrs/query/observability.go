package query

import (
	"context"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/internal/cqrsutil"
)

type observabilityOptions struct {
	idGen func() string
}

// ObservabilityOption configures ObservabilityMiddleware.
type ObservabilityOption func(*observabilityOptions)

// WithIDGenerator overrides the default uuid.NewString generator used when initializing
// a missing TraceID. Nil functions are ignored.
func WithIDGenerator(fn func() string) ObservabilityOption {
	return func(o *observabilityOptions) {
		if fn != nil {
			o.idGen = fn
		}
	}
}

// ObservabilityMiddleware ensures an active TraceID exists on ctx during query execution,
// preserving any incoming TraceID, CommandID, and CausationID without allocating new
// CommandID values.
func ObservabilityMiddleware(opts ...ObservabilityOption) Middleware {
	cfg := observabilityOptions{
		idGen: uuid.NewString,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	nextID := cqrsutil.NewIDGenerator(cfg.idGen)

	return func(next UntypedFunc) UntypedFunc {
		return func(ctx context.Context, q any) (any, error) {
			if existing, ok := cqrs.ObservabilityFromContext(ctx); ok && existing.TraceID != "" {
				return next(ctx, q)
			}
			childCtx := cqrs.WithTraceID(ctx, nextID())
			return next(childCtx, q)
		}
	}
}
