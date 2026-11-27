package command

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/internal/cqrsutil"
)

// Handler executes a strongly typed command C within a service boundary.
// Handlers return error so middlewares (transactions, retries, observability) can react reliably.
type Handler[C any] interface {
	Handle(ctx context.Context, cmd C) error
}

// Func adapts an ordinary function into a Handler[C].
type Func[C any] func(ctx context.Context, cmd C) error

// Handle invokes f(ctx, cmd).
func (f Func[C]) Handle(ctx context.Context, cmd C) error {
	return f(ctx, cmd)
}

// UntypedFunc executes a type-erased command payload inside middleware chains.
type UntypedFunc func(ctx context.Context, cmd any) error

// Middleware wraps command execution with cross-cutting concerns.
type Middleware func(next UntypedFunc) UntypedFunc

// Chain composes non-nil middlewares in outermost-first execution order.
func Chain(mws ...Middleware) Middleware {
	return cqrsutil.Chain(mws...)
}

// Wrap decorates typed handler h with non-generic middlewares while preserving its
// strongly typed Handler[C] interface.
func Wrap[C any](h Handler[C], mws ...Middleware) (Handler[C], error) {
	if cqrsutil.IsNilValue(h) {
		return nil, cqrs.ErrNilHandler
	}
	chain := Chain(mws...)
	wrapped := chain(func(ctx context.Context, raw any) error {
		cmd, ok := raw.(C)
		if !ok {
			return fmt.Errorf("command: unexpected type %T", raw)
		}
		return h.Handle(ctx, cmd)
	})
	return Func[C](func(ctx context.Context, cmd C) error {
		return wrapped(ctx, cmd)
	}), nil
}

type dispatchPolicy uint8

const (
	policyMaskRejections dispatchPolicy = iota
	policyStrictOpaque
	policyPassthrough
)

type busConfig struct {
	classifier  func(error) error
	onRejected  func(ctx context.Context, cmd any, err error)
	onError     func(ctx context.Context, cmd any, err error)
	middlewares []Middleware
	policy      dispatchPolicy
	strictInit  bool
}

// BusOption configures a Bus.
type BusOption func(*busConfig)

// WithMiddleware attaches middlewares applied to every command dispatched through the bus,
// executing outermost-first around per-command middlewares.
func WithMiddleware(mws ...Middleware) BusOption {
	return func(c *busConfig) {
		c.middlewares = append(c.middlewares, mws...)
	}
}

// WithErrorClassifier maps domain errors returned by handlers (for example wrapping
// domain sentinels with Reject) under the default rejection-masking policy.
func WithErrorClassifier(fn func(error) error) BusOption {
	return func(c *busConfig) {
		c.classifier = fn
	}
}

// WithOnRejected registers an internal observer invoked whenever a command fails with a
// rejected error under the default rejection-masking policy.
func WithOnRejected(fn func(ctx context.Context, cmd any, err error)) BusOption {
	return func(c *busConfig) {
		c.onRejected = fn
	}
}

// WithStrictOpaqueDispatch masks all post-lookup handler and middleware errors cleanly,
// forwarding non-nil failures to onError and returning nil to Dispatch callers.
// NewBus rejects a nil onError callback with ErrNilErrorSink.
func WithStrictOpaqueDispatch(onError func(ctx context.Context, cmd any, err error)) BusOption {
	return func(c *busConfig) {
		c.policy = policyStrictOpaque
		c.onError = onError
		c.strictInit = true
	}
}

// WithPassthroughErrors configures Dispatch to return every handler error untouched.
func WithPassthroughErrors() BusOption {
	return func(c *busConfig) {
		c.policy = policyPassthrough
		c.strictInit = false
	}
}

// Bus dispatches concrete commands to typed handlers wrapped in shared middlewares
// with deterministic boundary error semantics.
type Bus struct {
	handlers   map[reflect.Type]UntypedFunc
	chain      Middleware
	classifier func(error) error
	onRejected func(ctx context.Context, cmd any, err error)
	onError    func(ctx context.Context, cmd any, err error)
	mu         sync.RWMutex
	policy     dispatchPolicy
}

// NewBus constructs a Bus configured with opts.
func NewBus(opts ...BusOption) (*Bus, error) {
	cfg := busConfig{
		policy: policyMaskRejections,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.policy == policyStrictOpaque && cfg.strictInit && cfg.onError == nil {
		return nil, ErrNilErrorSink
	}

	return &Bus{
		handlers:   make(map[reflect.Type]UntypedFunc),
		chain:      Chain(cfg.middlewares...),
		classifier: cfg.classifier,
		onRejected: cfg.onRejected,
		onError:    cfg.onError,
		policy:     cfg.policy,
	}, nil
}

// Register binds a handler for concrete command type C onto bus with optional
// command-specific middlewares. Returns cqrs.ErrInvalidMessageType if C is an interface
// type and cqrs.ErrDuplicateHandler if C is already registered.
func Register[C any](bus *Bus, h Handler[C], mws ...Middleware) error {
	if bus == nil {
		return cqrs.ErrNilBus
	}
	if cqrsutil.IsNilValue(h) {
		return cqrs.ErrNilHandler
	}
	cmdType, err := cqrsutil.ConcreteMessageType[C](cqrs.ErrInvalidMessageType)
	if err != nil {
		return err
	}

	routeChain := Chain(mws...)
	entry := bus.chain(routeChain(func(ctx context.Context, raw any) error {
		cmd, ok := raw.(C)
		if !ok {
			return fmt.Errorf("command: unexpected type %T", raw)
		}
		return h.Handle(ctx, cmd)
	}))

	bus.mu.Lock()
	defer bus.mu.Unlock()

	if _, exists := bus.handlers[cmdType]; exists {
		return fmt.Errorf("%w: %s", cqrs.ErrDuplicateHandler, cmdType)
	}
	bus.handlers[cmdType] = entry
	return nil
}

// MustRegister behaves like Register and panics on invalid registration.
func MustRegister[C any](bus *Bus, h Handler[C], mws ...Middleware) {
	if err := Register(bus, h, mws...); err != nil {
		panic(err)
	}
}

// Dispatch invokes the registered handler for cmd's exact dynamic type and enforces the
// configured boundary error policy. Callers must pass a non-nil ctx.
func (b *Bus) Dispatch(ctx context.Context, cmd any) error {
	if b == nil {
		return cqrs.ErrNilBus
	}
	if cqrsutil.IsNilMessage(cmd) {
		return ErrNilCommand
	}

	cmdType := reflect.TypeOf(cmd)
	b.mu.RLock()
	handler, ok := b.handlers[cmdType]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", cqrs.ErrNoHandler, cmdType)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	err := handler(ctx, cmd)
	if err == nil {
		return nil
	}

	switch b.policy {
	case policyPassthrough:
		return err
	case policyStrictOpaque:
		b.onError(ctx, cmd, err)
		return nil
	case policyMaskRejections:
		return b.maskRejection(ctx, cmd, err)
	default:
		return b.maskRejection(ctx, cmd, err)
	}
}

func (b *Bus) maskRejection(ctx context.Context, cmd any, err error) error {
	classified := err
	if b.classifier != nil {
		if mapped := b.classifier(err); mapped != nil {
			classified = mapped
		}
	}
	if IsRejected(classified) {
		if b.onRejected != nil {
			b.onRejected(ctx, cmd, classified)
		}
		return nil
	}
	return err
}
