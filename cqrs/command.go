package cqrs

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// CommandHandler executes a strongly typed command C within a service boundary.
// Handlers return error so middlewares (transactions, retries, observability) can react reliably.
type CommandHandler[C any] interface {
	Handle(ctx context.Context, cmd C) error
}

// CommandFunc adapts an ordinary function into a CommandHandler[C].
type CommandFunc[C any] func(ctx context.Context, cmd C) error

// Handle invokes f(ctx, cmd).
func (f CommandFunc[C]) Handle(ctx context.Context, cmd C) error {
	return f(ctx, cmd)
}

// UntypedCommandFunc executes a type-erased command payload inside middleware chains.
type UntypedCommandFunc func(ctx context.Context, cmd any) error

// CommandMiddleware wraps command execution with cross-cutting concerns.
type CommandMiddleware func(next UntypedCommandFunc) UntypedCommandFunc

// ChainCommandMiddleware composes non-nil middlewares in outermost-first execution order.
func ChainCommandMiddleware(mws ...CommandMiddleware) CommandMiddleware {
	filtered := make([]CommandMiddleware, 0, len(mws))
	for _, mw := range mws {
		if mw != nil {
			filtered = append(filtered, mw)
		}
	}
	return func(next UntypedCommandFunc) UntypedCommandFunc {
		wrapped := next
		for i := len(filtered) - 1; i >= 0; i-- {
			wrapped = filtered[i](wrapped)
		}
		return wrapped
	}
}

// WrapCommand decorates typed handler h with non-generic middlewares while preserving its
// strongly typed CommandHandler[C] interface.
func WrapCommand[C any](h CommandHandler[C], mws ...CommandMiddleware) (CommandHandler[C], error) {
	if isNilValue(h) {
		return nil, ErrNilHandler
	}
	chain := ChainCommandMiddleware(mws...)
	wrapped := chain(func(ctx context.Context, raw any) error {
		cmd, ok := raw.(C)
		if !ok {
			return fmt.Errorf("cqrs: unexpected command type %T", raw)
		}
		return h.Handle(ctx, cmd)
	})
	return CommandFunc[C](func(ctx context.Context, cmd C) error {
		return wrapped(normalizeContext(ctx), cmd)
	}), nil
}

type dispatchPolicy uint8

const (
	policyMaskRejections dispatchPolicy = iota
	policyStrictOpaque
	policyPassthrough
)

type commandBusConfig struct {
	classifier  func(error) error
	onRejected  func(ctx context.Context, cmd any, err error)
	onError     func(ctx context.Context, cmd any, err error)
	middlewares []CommandMiddleware
	policy      dispatchPolicy
	strictInit  bool
}

// CommandBusOption configures a CommandBus.
type CommandBusOption func(*commandBusConfig)

// WithCommandMiddleware attaches middlewares applied to every command dispatched through the bus,
// executing outermost-first around per-command middlewares.
func WithCommandMiddleware(mws ...CommandMiddleware) CommandBusOption {
	return func(c *commandBusConfig) {
		c.middlewares = append(c.middlewares, mws...)
	}
}

// WithCommandErrorClassifier maps domain errors returned by handlers (for example wrapping
// domain sentinels with Reject) under the default MaskRejections policy.
func WithCommandErrorClassifier(fn func(error) error) CommandBusOption {
	return func(c *commandBusConfig) {
		c.classifier = fn
	}
}

// WithOnRejected registers an internal observer invoked whenever a command fails with a
// rejected error under the default MaskRejections policy.
func WithOnRejected(fn func(ctx context.Context, cmd any, err error)) CommandBusOption {
	return func(c *commandBusConfig) {
		c.onRejected = fn
	}
}

// WithStrictOpaqueDispatch masks all post-lookup handler and middleware errors cleanly,
// forwarding non-nil failures to onError and returning nil to Dispatch callers.
// NewCommandBus rejects a nil onError callback with ErrNilErrorSink.
func WithStrictOpaqueDispatch(onError func(ctx context.Context, cmd any, err error)) CommandBusOption {
	return func(c *commandBusConfig) {
		c.policy = policyStrictOpaque
		c.onError = onError
		c.strictInit = true
	}
}

// WithPassthroughErrors configures Dispatch to return every handler error untouched.
func WithPassthroughErrors() CommandBusOption {
	return func(c *commandBusConfig) {
		c.policy = policyPassthrough
		c.strictInit = false
	}
}

// CommandBus dispatches concrete commands to typed handlers wrapped in shared middlewares
// with deterministic boundary error semantics.
type CommandBus struct {
	handlers   map[reflect.Type]UntypedCommandFunc
	chain      CommandMiddleware
	classifier func(error) error
	onRejected func(ctx context.Context, cmd any, err error)
	onError    func(ctx context.Context, cmd any, err error)
	mu         sync.RWMutex
	policy     dispatchPolicy
}

// NewCommandBus constructs a CommandBus configured with opts.
func NewCommandBus(opts ...CommandBusOption) (*CommandBus, error) {
	cfg := commandBusConfig{
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

	return &CommandBus{
		handlers:   make(map[reflect.Type]UntypedCommandFunc),
		chain:      ChainCommandMiddleware(cfg.middlewares...),
		classifier: cfg.classifier,
		onRejected: cfg.onRejected,
		onError:    cfg.onError,
		policy:     cfg.policy,
	}, nil
}

// RegisterCommand binds a handler for concrete command type C onto bus with optional
// command-specific middlewares. Returns ErrInvalidMessageType if C is an interface type
// and ErrDuplicateHandler if C is already registered.
func RegisterCommand[C any](bus *CommandBus, h CommandHandler[C], mws ...CommandMiddleware) error {
	if bus == nil {
		return ErrNilBus
	}
	if isNilValue(h) {
		return ErrNilHandler
	}
	cmdType, err := concreteMessageType[C]()
	if err != nil {
		return err
	}

	routeChain := ChainCommandMiddleware(mws...)
	entry := bus.chain(routeChain(func(ctx context.Context, raw any) error {
		cmd, ok := raw.(C)
		if !ok {
			return fmt.Errorf("cqrs: unexpected command type %T", raw)
		}
		return h.Handle(ctx, cmd)
	}))

	bus.mu.Lock()
	defer bus.mu.Unlock()

	if _, exists := bus.handlers[cmdType]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHandler, cmdType)
	}
	bus.handlers[cmdType] = entry
	return nil
}

// MustRegisterCommand behaves like RegisterCommand and panics on invalid registration.
func MustRegisterCommand[C any](bus *CommandBus, h CommandHandler[C], mws ...CommandMiddleware) {
	if err := RegisterCommand(bus, h, mws...); err != nil {
		panic(err)
	}
}

// Dispatch invokes the registered handler for cmd's exact dynamic type and enforces the
// configured boundary error policy.
func (b *CommandBus) Dispatch(ctx context.Context, cmd any) error {
	if b == nil {
		return ErrNilBus
	}
	if isNilMessage(cmd) {
		return ErrNilCommand
	}

	cmdType := reflect.TypeOf(cmd)
	b.mu.RLock()
	handler, ok := b.handlers[cmdType]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrNoHandler, cmdType)
	}

	ctx = normalizeContext(ctx)
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
		return b.handleMaskRejections(ctx, cmd, err)
	default:
		return b.handleMaskRejections(ctx, cmd, err)
	}
}

func (b *CommandBus) handleMaskRejections(ctx context.Context, cmd any, err error) error {
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

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func concreteMessageType[T any]() (reflect.Type, error) {
	rt := reflect.TypeFor[T]()
	if rt.Kind() == reflect.Interface {
		return nil, ErrInvalidMessageType
	}
	return rt, nil
}

func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func isNilMessage(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		return rv.IsNil()
	default:
		return false
	}
}
