package cqrs_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"eventsalsa.dev/cqrs"
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

func TestWrapCommandAndNilGuards(t *testing.T) {
	t.Parallel()

	if _, err := cqrs.WrapCommand[testCreateCmd](nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	var nilFn cqrs.CommandFunc[testCreateCmd]
	if _, err := cqrs.WrapCommand[testCreateCmd](nilFn); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler for typed nil func", err)
	}
	var nilStruct *typedNilHandler
	if _, err := cqrs.WrapCommand[testCreateCmd](nilStruct); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler for typed nil struct ptr", err)
	}

	var order []string
	mw := func(name string) cqrs.CommandMiddleware {
		return func(next cqrs.UntypedCommandFunc) cqrs.UntypedCommandFunc {
			return func(ctx context.Context, cmd any) error {
				order = append(order, name+":before")
				err := next(ctx, cmd)
				order = append(order, name+":after")
				return err
			}
		}
	}

	wrapped, err := cqrs.WrapCommand(
		cqrs.CommandFunc[testCreateCmd](func(_ context.Context, cmd testCreateCmd) error {
			order = append(order, "handler:"+cmd.ID)
			return nil
		}),
		mw("outer"),
		nil,
		mw("inner"),
	)
	if err != nil {
		t.Fatalf("WrapCommand: %v", err)
	}

	//nolint:staticcheck // SA1012: intentionally testing nil context normalization
	if handleErr := wrapped.Handle(nil, testCreateCmd{ID: "c1"}); handleErr != nil {
		t.Fatalf("Handle: %v", handleErr)
	}

	want := []string{"outer:before", "inner:before", "handler:c1", "inner:after", "outer:after"}
	if !slices.Equal(order, want) {
		t.Fatalf("got order %v, want %v", order, want)
	}
}

func TestCommandBus_RegistrationAndPreDispatchFailures(t *testing.T) {
	t.Parallel()

	var nilBus *cqrs.CommandBus
	if err := cqrs.RegisterCommand(nilBus, cqrs.CommandFunc[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	})); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("got %v, want ErrNilBus", err)
	}
	if err := nilBus.Dispatch(context.Background(), testCreateCmd{}); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("nilBus.Dispatch got %v, want ErrNilBus", err)
	}

	bus, err := cqrs.NewCommandBus(nil)
	if err != nil {
		t.Fatalf("NewCommandBus: %v", err)
	}

	if err := cqrs.RegisterCommand[testCreateCmd](bus, nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	if err := cqrs.RegisterCommand(bus, cqrs.CommandFunc[testCmdInterface](func(context.Context, testCmdInterface) error {
		return nil
	})); !errors.Is(err, cqrs.ErrInvalidMessageType) {
		t.Fatalf("interface type registration got %v, want ErrInvalidMessageType", err)
	}

	noop := cqrs.CommandFunc[testCreateCmd](func(context.Context, testCreateCmd) error { return nil })
	if err := cqrs.RegisterCommand(bus, noop); err != nil {
		t.Fatalf("RegisterCommand: %v", err)
	}
	if err := cqrs.RegisterCommand(bus, noop); !errors.Is(err, cqrs.ErrDuplicateHandler) {
		t.Fatalf("duplicate RegisterCommand got %v, want ErrDuplicateHandler", err)
	}

	// Pre-dispatch checks: nil, typed-nil ptr, value-vs-pointer mismatch, pre-canceled ctx
	if err := bus.Dispatch(context.Background(), nil); !errors.Is(err, cqrs.ErrNilCommand) {
		t.Fatalf("nil command got %v, want ErrNilCommand", err)
	}
	var nilPtrCmd *testCreateCmd
	if err := bus.Dispatch(context.Background(), nilPtrCmd); !errors.Is(err, cqrs.ErrNilCommand) {
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
}

func TestCommandBus_MustRegisterCommandPanicsOnInvalid(t *testing.T) {
	t.Parallel()

	bus, err := cqrs.NewCommandBus()
	if err != nil {
		t.Fatalf("NewCommandBus: %v", err)
	}
	cqrs.MustRegisterCommand(bus, cqrs.CommandFunc[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	}))

	defer func() {
		if recover() == nil {
			t.Fatal("expected MustRegisterCommand to panic on duplicate registration")
		}
	}()
	cqrs.MustRegisterCommand(bus, cqrs.CommandFunc[testCreateCmd](func(context.Context, testCreateCmd) error {
		return nil
	}))
}

func TestCommandBus_ErrorPoliciesAndMiddlewareOrder(t *testing.T) {
	t.Parallel()

	type domainCmd struct{ Fail string }

	errDomain := errors.New("guest not verified")
	errInfra := errors.New("database unreachable")

	t.Run("default MaskRejections masks Reject/classified rejections and surfaces infra faults", func(t *testing.T) {
		t.Parallel()

		var rejectedSeen []error
		var mwTrace []string
		bus, err := cqrs.NewCommandBus(
			cqrs.WithCommandMiddleware(func(next cqrs.UntypedCommandFunc) cqrs.UntypedCommandFunc {
				return func(ctx context.Context, cmd any) error {
					mwTrace = append(mwTrace, "bus")
					return next(ctx, cmd)
				}
			}),
			cqrs.WithCommandErrorClassifier(func(e error) error {
				if errors.Is(e, errDomain) {
					return cqrs.Reject(e)
				}
				return e
			}),
			cqrs.WithOnRejected(func(_ context.Context, _ any, e error) {
				rejectedSeen = append(rejectedSeen, e)
			}),
		)
		if err != nil {
			t.Fatalf("NewCommandBus: %v", err)
		}

		cqrs.MustRegisterCommand(bus, cqrs.CommandFunc[domainCmd](func(_ context.Context, c domainCmd) error {
			mwTrace = append(mwTrace, "handler")
			switch c.Fail {
			case "classified":
				return errDomain
			case "marked":
				return cqrs.Reject(errors.New("explicit rejection"))
			case "infra":
				return errInfra
			default:
				return nil
			}
		}), func(next cqrs.UntypedCommandFunc) cqrs.UntypedCommandFunc {
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

		if _, err := cqrs.NewCommandBus(cqrs.WithStrictOpaqueDispatch(nil)); !errors.Is(err, cqrs.ErrNilErrorSink) {
			t.Fatalf("nil sink got %v, want ErrNilErrorSink", err)
		}

		var sunk []error
		bus, err := cqrs.NewCommandBus(cqrs.WithStrictOpaqueDispatch(func(_ context.Context, _ any, e error) {
			sunk = append(sunk, e)
		}))
		if err != nil {
			t.Fatalf("NewCommandBus: %v", err)
		}

		cqrs.MustRegisterCommand(bus, cqrs.CommandFunc[domainCmd](func(_ context.Context, c domainCmd) error {
			if c.Fail == "infra" {
				return errInfra
			}
			return cqrs.Reject(errDomain)
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

		bus, err := cqrs.NewCommandBus(cqrs.WithPassthroughErrors())
		if err != nil {
			t.Fatalf("NewCommandBus: %v", err)
		}
		cqrs.MustRegisterCommand(bus, cqrs.CommandFunc[domainCmd](func(_ context.Context, _ domainCmd) error {
			return cqrs.Reject(errDomain)
		}))

		got := bus.Dispatch(context.Background(), domainCmd{})
		if !errors.Is(got, errDomain) || !cqrs.IsRejected(got) {
			t.Fatalf("expected raw rejected errDomain, got %v", got)
		}
	})
}
