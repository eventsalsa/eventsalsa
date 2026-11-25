package cqrs

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// QueryHandler executes a read-only query Q and returns read projection R.
type QueryHandler[Q any, R any] interface {
	Handle(ctx context.Context, query Q) (R, error)
}

// QueryFunc adapts an ordinary function into a QueryHandler[Q, R].
type QueryFunc[Q any, R any] func(ctx context.Context, query Q) (R, error)

// Handle invokes f(ctx, query).
func (f QueryFunc[Q, R]) Handle(ctx context.Context, query Q) (R, error) {
	return f(ctx, query)
}

// UntypedQueryFunc executes a type-erased query payload inside query middleware chains.
type UntypedQueryFunc func(ctx context.Context, query any) (any, error)

// QueryMiddleware wraps query execution with read-side concerns (timeouts, metrics, read-tx).
type QueryMiddleware func(next UntypedQueryFunc) UntypedQueryFunc

// ChainQueryMiddleware composes non-nil query middlewares in outermost-first order.
func ChainQueryMiddleware(mws ...QueryMiddleware) QueryMiddleware {
	filtered := make([]QueryMiddleware, 0, len(mws))
	for _, mw := range mws {
		if mw != nil {
			filtered = append(filtered, mw)
		}
	}
	return func(next UntypedQueryFunc) UntypedQueryFunc {
		wrapped := next
		for i := len(filtered) - 1; i >= 0; i-- {
			wrapped = filtered[i](wrapped)
		}
		return wrapped
	}
}

// WrapQuery decorates typed query handler h with non-generic middlewares while preserving
// its strongly typed QueryHandler[Q, R] interface.
func WrapQuery[Q any, R any](h QueryHandler[Q, R], mws ...QueryMiddleware) (QueryHandler[Q, R], error) {
	if isNilValue(h) {
		return nil, ErrNilHandler
	}
	chain := ChainQueryMiddleware(mws...)
	wrapped := chain(func(ctx context.Context, raw any) (any, error) {
		q, ok := raw.(Q)
		if !ok {
			return nil, fmt.Errorf("cqrs: unexpected query type %T", raw)
		}
		return h.Handle(ctx, q)
	})
	return QueryFunc[Q, R](func(ctx context.Context, query Q) (R, error) {
		raw, err := wrapped(normalizeContext(ctx), query)
		if err != nil {
			var zero R
			return zero, err
		}
		return convertQueryResult[R](raw)
	}), nil
}

type queryBusConfig struct {
	middlewares []QueryMiddleware
}

// QueryBusOption configures a QueryBus.
type QueryBusOption func(*queryBusConfig)

// WithQueryMiddleware attaches middlewares applied to every query executed via QueryBus,
// executing outermost-first around per-query middlewares.
func WithQueryMiddleware(mws ...QueryMiddleware) QueryBusOption {
	return func(c *queryBusConfig) {
		c.middlewares = append(c.middlewares, mws...)
	}
}

type queryEntry struct {
	resultType reflect.Type
	fn         UntypedQueryFunc
}

// QueryBus routes strongly typed queries Q to registered read-side handlers through shared middleware.
type QueryBus struct {
	handlers map[reflect.Type]queryEntry
	chain    QueryMiddleware
	mu       sync.RWMutex
}

// NewQueryBus constructs a QueryBus configured with opts.
func NewQueryBus(opts ...QueryBusOption) (*QueryBus, error) {
	var cfg queryBusConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &QueryBus{
		handlers: make(map[reflect.Type]queryEntry),
		chain:    ChainQueryMiddleware(cfg.middlewares...),
	}, nil
}

// RegisterQuery binds a handler for concrete query type Q returning R onto bus with optional
// query-specific middlewares.
func RegisterQuery[Q any, R any](bus *QueryBus, h QueryHandler[Q, R], mws ...QueryMiddleware) error {
	if bus == nil {
		return ErrNilBus
	}
	if isNilValue(h) {
		return ErrNilHandler
	}
	qType, err := concreteMessageType[Q]()
	if err != nil {
		return err
	}

	routeChain := ChainQueryMiddleware(mws...)
	fn := bus.chain(routeChain(func(ctx context.Context, raw any) (any, error) {
		q, ok := raw.(Q)
		if !ok {
			return nil, fmt.Errorf("cqrs: unexpected query type %T", raw)
		}
		return h.Handle(ctx, q)
	}))

	bus.mu.Lock()
	defer bus.mu.Unlock()

	if _, exists := bus.handlers[qType]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHandler, qType)
	}
	bus.handlers[qType] = queryEntry{
		resultType: reflect.TypeFor[R](),
		fn:         fn,
	}
	return nil
}

// MustRegisterQuery behaves like RegisterQuery and panics on invalid registration.
func MustRegisterQuery[Q any, R any](bus *QueryBus, h QueryHandler[Q, R], mws ...QueryMiddleware) {
	if err := RegisterQuery(bus, h, mws...); err != nil {
		panic(err)
	}
}

// Query dispatches query q onto bus and returns typed result R. Put R first so callers specify
// only R while Q is inferred automatically: cqrs.Query[Profile](ctx, bus, GetProfile{ID: "1"}).
func Query[R any, Q any](ctx context.Context, bus *QueryBus, q Q) (R, error) {
	var zero R
	if bus == nil {
		return zero, ErrNilBus
	}
	if isNilMessage(q) {
		return zero, ErrNilQuery
	}

	qType := reflect.TypeOf(q)
	bus.mu.RLock()
	entry, ok := bus.handlers[qType]
	bus.mu.RUnlock()
	if !ok {
		return zero, fmt.Errorf("%w: %s", ErrNoHandler, qType)
	}

	wantR := reflect.TypeFor[R]()
	if entry.resultType != wantR {
		return zero, fmt.Errorf("%w: registered %s, requested %s", ErrInvalidResultType, entry.resultType, wantR)
	}

	ctx = normalizeContext(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return zero, ctxErr
	}

	raw, err := entry.fn(ctx, q)
	if err != nil {
		return zero, err
	}
	return convertQueryResult[R](raw)
}

func convertQueryResult[R any](raw any) (R, error) {
	var zero R
	if raw == nil {
		return zero, nil
	}
	typed, ok := raw.(R)
	if !ok {
		return zero, fmt.Errorf("%w: got %T, want %T", ErrInvalidResultType, raw, zero)
	}
	return typed, nil
}
