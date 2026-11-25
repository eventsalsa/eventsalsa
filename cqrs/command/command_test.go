package command_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
)

type testCreateCmd struct {
	ID string
}

type testCmdInterface interface {
	isTestCmd()
}

type typedNilHandler struct{}

func (h *typedNilHandler) Handle(_ context.Context, _ testCreateCmd) error {
	return nil
}

func TestWrapAndNilGuards(t *testing.T) {
	t.Parallel()

	if _, err := command.Wrap[testCreateCmd](nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	var nilFn command.Func[testCreateCmd]
	if _, err := command.Wrap[testCreateCmd](nilFn); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler for typed nil func", err)
	}
	var nilStruct *typedNilHandler
	if _, err := command.Wrap[testCreateCmd](nilStruct); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler for typed nil struct ptr", err)
	}

	var order []string
	mw := func(name string) command.Middleware {
		return func(next command.UntypedFunc) command.UntypedFunc {
			return func(ctx context.Context, cmd any) error {
				order = append(order, name+":before")
				err := next(ctx, cmd)
				order = append(order, name+":after")
				return err
			}
		}
	}

	wrapped, err := command.Wrap(
		command.Func[testCreateCmd](func(_ context.Context, cmd testCreateCmd) error {
			order = append(order, "handler:"+cmd.ID)
			return nil
		}),
		mw("outer"),
		nil,
		mw("inner"),
	)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	if handleErr := wrapped.Handle(context.Background(), testCreateCmd{ID: "c1"}); handleErr != nil {
		t.Fatalf("Handle: %v", handleErr)
	}

	want := []string{"outer:before", "inner:before", "handler:c1", "inner:after", "outer:after"}
	if !slices.Equal(order, want) {
		t.Fatalf("got order %v, want %v", order, want)
	}
}

func TestBus_RegistrationAndPreDispatchFailures(t *testing.T) {
	t.Parallel()

	var nilBus *command.Bus
	if err := command.Register(nilBus, command.Func[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	})); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("got %v, want ErrNilBus", err)
	}
	if err := nilBus.Dispatch(context.Background(), testCreateCmd{}); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("nilBus.Dispatch got %v, want ErrNilBus", err)
	}

	bus, err := command.NewBus(nil)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	if err := command.Register[testCreateCmd](bus, nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	if err := command.Register(bus, command.Func[testCmdInterface](func(context.Context, testCmdInterface) error {
		return nil
	})); !errors.Is(err, cqrs.ErrInvalidMessageType) {
		t.Fatalf("interface type registration got %v, want ErrInvalidMessageType", err)
	}

	noop := command.Func[testCreateCmd](func(context.Context, testCreateCmd) error { return nil })
	if err := command.Register(bus, noop); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := command.Register(bus, noop); !errors.Is(err, cqrs.ErrDuplicateHandler) {
		t.Fatalf("duplicate Register got %v, want ErrDuplicateHandler", err)
	}

	// Pre-dispatch checks: nil, typed-nil ptr, value-vs-pointer mismatch, pre-canceled ctx, nil ctx panic
	if err := bus.Dispatch(context.Background(), nil); !errors.Is(err, command.ErrNilCommand) {
		t.Fatalf("nil command got %v, want ErrNilCommand", err)
	}
	var nilPtrCmd *testCreateCmd
	if err := bus.Dispatch(context.Background(), nilPtrCmd); !errors.Is(err, command.ErrNilCommand) {
		t.Fatalf("typed nil pointer got %v, want ErrNilCommand", err)
	}
	if err := bus.Dispatch(context.Background(), &testCreateCmd{ID: "ptr"}); !errors.Is(err, cqrs.ErrNoHandler) {
		t.Fatalf("pointer mismatch got %v, want ErrNoHandler", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.Dispatch(ctx, testCreateCmd{ID: "1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx got %v, want context.Canceled", err)
	}

	t.Run("nil ctx fails loudly instead of defaulting to Background", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recover() == nil {
				t.Fatal("expected Dispatch(nil, ...) to panic")
			}
		}()
		//nolint:staticcheck // SA1012: verifying strict non-nil context contract
		if dErr := bus.Dispatch(nil, testCreateCmd{ID: "nil-ctx"}); dErr != nil {
			t.Fatalf("expected panic on nil ctx, got err=%v", dErr)
		}
	})
}

func TestBus_MustRegisterPanicsOnInvalid(t *testing.T) {
	t.Parallel()

	bus, err := command.NewBus()
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, command.Func[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	}))

	defer func() {
		if recover() == nil {
			t.Fatal("expected MustRegister to panic on duplicate registration")
		}
	}()
	command.MustRegister(bus, command.Func[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	}))
}

func TestBus_ErrorPoliciesAndMiddlewareOrder(t *testing.T) {
	t.Parallel()

	type domainCmd struct{ Fail string }

	errDomain := errors.New("guest not verified")
	errInfra := errors.New("database unreachable")

	t.Run("default policy masks Reject/classified rejections and surfaces infra faults", func(t *testing.T) {
		t.Parallel()

		var rejectedSeen []error
		var mwTrace []string
		bus, err := command.NewBus(
			command.WithMiddleware(func(next command.UntypedFunc) command.UntypedFunc {
				return func(ctx context.Context, cmd any) error {
					mwTrace = append(mwTrace, "bus")
					return next(ctx, cmd)
				}
			}),
			command.WithErrorClassifier(func(e error) error {
				if errors.Is(e, errDomain) {
					return command.Reject(e)
				}
				return e
			}),
			command.WithOnRejected(func(_ context.Context, _ any, e error) {
				rejectedSeen = append(rejectedSeen, e)
			}),
		)
		if err != nil {
			t.Fatalf("NewBus: %v", err)
		}

		command.MustRegister(bus, command.Func[domainCmd](func(_ context.Context, c domainCmd) error {
			mwTrace = append(mwTrace, "handler")
			switch c.Fail {
			case "classified":
				return errDomain
			case "marked":
				return command.Reject(errors.New("explicit rejection"))
			case "infra":
				return errInfra
			default:
				return nil
			}
		}), func(next command.UntypedFunc) command.UntypedFunc {
			return func(ctx context.Context, cmd any) error {
				mwTrace = append(mwTrace, "route")
				return next(ctx, cmd)
			}
		})

		if dErr := bus.Dispatch(context.Background(), domainCmd{Fail: "classified"}); dErr != nil {
			t.Fatalf("classified domain rejection must return nil, got %v", dErr)
		}
		if !slices.Equal(mwTrace, []string{"bus", "route", "handler"}) {
			t.Fatalf("unexpected middleware execution order: %v", mwTrace)
		}
		if dErr := bus.Dispatch(context.Background(), domainCmd{Fail: "marked"}); dErr != nil {
			t.Fatalf("explicit Reject error must return nil, got %v", dErr)
		}
		if len(rejectedSeen) != 2 || !errors.Is(rejectedSeen[0], errDomain) {
			t.Fatalf("expected 2 rejections in hook, got %v", rejectedSeen)
		}

		infraErr := bus.Dispatch(context.Background(), domainCmd{Fail: "infra"})
		if !errors.Is(infraErr, errInfra) {
			t.Fatalf("infra fault must surface, got %v", infraErr)
		}
		if len(rejectedSeen) != 2 {
			t.Fatalf("infra fault must not invoke onRejected, got %d", len(rejectedSeen))
		}
	})

	t.Run("StrictOpaque requires non-nil sink and masks all post-lookup errors", func(t *testing.T) {
		t.Parallel()

		if _, err := command.NewBus(command.WithStrictOpaqueDispatch(nil)); !errors.Is(err, command.ErrNilErrorSink) {
			t.Fatalf("nil sink got %v, want ErrNilErrorSink", err)
		}

		var sunk []error
		bus, err := command.NewBus(command.WithStrictOpaqueDispatch(func(_ context.Context, _ any, e error) {
			sunk = append(sunk, e)
		}))
		if err != nil {
			t.Fatalf("NewBus: %v", err)
		}

		command.MustRegister(bus, command.Func[domainCmd](func(_ context.Context, c domainCmd) error {
			if c.Fail == "infra" {
				return errInfra
			}
			return command.Reject(errDomain)
		}))

		if dErr := bus.Dispatch(context.Background(), domainCmd{Fail: "infra"}); dErr != nil {
			t.Fatalf("strict opaque must return nil on infra error, got %v", dErr)
		}
		if dErr := bus.Dispatch(context.Background(), domainCmd{Fail: "domain"}); dErr != nil {
			t.Fatalf("strict opaque must return nil on domain error, got %v", dErr)
		}
		if len(sunk) != 2 || !errors.Is(sunk[0], errInfra) || !errors.Is(sunk[1], errDomain) {
			t.Fatalf("expected both errors in sink, got %v", sunk)
		}
	})

	t.Run("PassthroughErrors returns every error untouched", func(t *testing.T) {
		t.Parallel()

		bus, err := command.NewBus(command.WithPassthroughErrors())
		if err != nil {
			t.Fatalf("NewBus: %v", err)
		}
		command.MustRegister(bus, command.Func[domainCmd](func(_ context.Context, _ domainCmd) error {
			return command.Reject(errDomain)
		}))

		got := bus.Dispatch(context.Background(), domainCmd{})
		if !errors.Is(got, errDomain) || !command.IsRejected(got) {
			t.Fatalf("expected raw rejected errDomain, got %v", got)
		}
	})
}
