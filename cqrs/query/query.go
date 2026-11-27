package query

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/internal/cqrsutil"
)

// Handler executes a read-only query Q and returns read projection R.
type Handler[Q any, R any] interface {
	Handle(ctx context.Context, query Q) (R, error)
}

// Func adapts an ordinary function into a Handler[Q, R].
type Func[Q any, R any] func(ctx context.Context, query Q) (R, error)

// Handle invokes f(ctx, query).
func (f Func[Q, R]) Handle(ctx context.Context, query Q) (R, error) {
	return f(ctx, query)
}

// UntypedFunc executes a type-erased query payload inside query middleware chains.
type UntypedFunc func(ctx context.Context, query any) (any, error)

// Middleware wraps query execution with read-side concerns (timeouts, metrics, read-tx, tracing).
type Middleware func(next UntypedFunc) UntypedFunc

// Chain composes non-nil query middlewares in outermost-first execution order.
func Chain(mws ...Middleware) Middleware {
	return cqrsutil.Chain(mws...)
}

// Wrap decorates typed handler h with non-generic middlewares while preserving its
// strongly typed Handler[Q, R] interface.
func Wrap[Q any, R any](h Handler[Q, R], mws ...Middleware) (Handler[Q, R], error) {
	if cqrsutil.IsNilValue(h) {
		return nil, cqrs.ErrNilHandler
	}
	chain := Chain(mws...)
	wrapped := chain(func(ctx context.Context, raw any) (any, error) {
		q, ok := raw.(Q)
		if !ok {
			return nil, fmt.Errorf("query: unexpected type %T", raw)
		}
		return h.Handle(ctx, q)
	})
	return Func[Q, R](func(ctx context.Context, q Q) (R, error) {
		raw, err := wrapped(ctx, q)
		if err != nil {
			var zero R
			return zero, err
		}
		return cqrsutil.ConvertResult[R](raw, ErrInvalidResultType)
	}), nil
}

type busConfig struct {
	middlewares []Middleware
}

// BusOption configures a Bus.
type BusOption func(*busConfig)

// WithMiddleware attaches middlewares applied to every query executed via Bus,
// executing outermost-first around per-query middlewares.
func WithMiddleware(mws ...Middleware) BusOption {
	return func(c *busConfig) {
		c.middlewares = append(c.middlewares, mws...)
	}
}

type queryEntry struct {
	resultType reflect.Type
	fn         UntypedFunc
}

// Bus routes strongly typed queries Q to registered read-side handlers through shared middleware.
type Bus struct {
	handlers map[reflect.Type]queryEntry
	chain    Middleware
	mu       sync.RWMutex
}

// NewBus constructs a Bus configured with opts.
func NewBus(opts ...BusOption) (*Bus, error) {
	var cfg busConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Bus{
		handlers: make(map[reflect.Type]queryEntry),
		chain:    Chain(cfg.middlewares...),
	}, nil
}

// Register binds a handler for concrete query type Q returning R onto bus with optional
// query-specific middlewares. Returns cqrs.ErrInvalidMessageType if Q is an interface type
// and cqrs.ErrDuplicateHandler if Q is already registered.
func Register[Q any, R any](bus *Bus, h Handler[Q, R], mws ...Middleware) error {
	if bus == nil {
		return cqrs.ErrNilBus
	}
	if cqrsutil.IsNilValue(h) {
		return cqrs.ErrNilHandler
	}
	qType, err := cqrsutil.ConcreteMessageType[Q](cqrs.ErrInvalidMessageType)
	if err != nil {
		return err
	}

	routeChain := Chain(mws...)
	fn := bus.chain(routeChain(func(ctx context.Context, raw any) (any, error) {
		q, ok := raw.(Q)
		if !ok {
			return nil, fmt.Errorf("query: unexpected type %T", raw)
		}
		return h.Handle(ctx, q)
	}))

	bus.mu.Lock()
	defer bus.mu.Unlock()

	if _, exists := bus.handlers[qType]; exists {
		return fmt.Errorf("%w: %s", cqrs.ErrDuplicateHandler, qType)
	}
	bus.handlers[qType] = queryEntry{
		resultType: reflect.TypeFor[R](),
		fn:         fn,
	}
	return nil
}

// MustRegister behaves like Register and panics on invalid registration.
func MustRegister[Q any, R any](bus *Bus, h Handler[Q, R], mws ...Middleware) {
	if err := Register(bus, h, mws...); err != nil {
		panic(err)
	}
}

// Execute dispatches query q onto bus and returns typed result R. Put R first so callers specify
// only R while Q is inferred automatically: query.Execute[Profile](ctx, bus, GetProfile{ID: "1"}).
// Callers must pass a non-nil ctx.
func Execute[R any, Q any](ctx context.Context, bus *Bus, q Q) (R, error) {
	var zero R
	if bus == nil {
		return zero, cqrs.ErrNilBus
	}
	if cqrsutil.IsNilMessage(q) {
		return zero, ErrNilQuery
	}

	qType := reflect.TypeOf(q)
	bus.mu.RLock()
	entry, ok := bus.handlers[qType]
	bus.mu.RUnlock()
	if !ok {
		return zero, fmt.Errorf("%w: %s", cqrs.ErrNoHandler, qType)
	}

	wantR := reflect.TypeFor[R]()
	if entry.resultType != wantR {
		return zero, fmt.Errorf("%w: registered %s, requested %s", ErrInvalidResultType, entry.resultType, wantR)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return zero, ctxErr
	}

	raw, err := entry.fn(ctx, q)
	if err != nil {
		return zero, err
	}
	return cqrsutil.ConvertResult[R](raw, ErrInvalidResultType)
}
