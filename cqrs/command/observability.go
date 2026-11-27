package command

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

// WithIDGenerator overrides the default uuid.NewString generator used when minting
// CommandID and missing TraceID values. Nil functions are ignored.
func WithIDGenerator(fn func() string) ObservabilityOption {
	return func(o *observabilityOptions) {
		if fn != nil {
			o.idGen = fn
		}
	}
}

// ObservabilityMiddleware stamps a fresh CommandID onto ctx for each command execution,
// preserves or initializes TraceID, and promotes any parent ctx CommandID into CausationID.
//
// Mount ObservabilityMiddleware OUTSIDE RetryMiddleware so retries share the same CommandID.
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
		return func(ctx context.Context, cmd any) error {
			existing, _ := cqrs.ObservabilityFromContext(ctx)

			causationID := existing.CausationID
			if existing.CommandID != "" {
				causationID = existing.CommandID
			}
			traceID := existing.TraceID
			if traceID == "" {
				traceID = nextID()
			}

			childCtx := cqrs.WithObservability(ctx, cqrs.Observability{
				CommandID:   nextID(),
				TraceID:     traceID,
				CausationID: causationID,
			})
			return next(childCtx, cmd)
		}
	}
}
