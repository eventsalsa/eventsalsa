package query_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/query"
)

type testProfileQuery struct {
	ID string
}

type testProfileResult struct {
	Name string
}

type wrongProfileResult struct {
	Other int
}

type testQueryInterface interface {
	isQuery()
}

type nilQueryHandler struct{}

func (h *nilQueryHandler) Handle(_ context.Context, _ testProfileQuery) (testProfileResult, error) {
	return testProfileResult{}, nil
}

func TestWrapAndNilGuards(t *testing.T) {
	t.Parallel()

	if _, err := query.Wrap[testProfileQuery, testProfileResult](nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	var nilFn query.Func[testProfileQuery, testProfileResult]
	if _, err := query.Wrap[testProfileQuery, testProfileResult](nilFn); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("typed nil func got %v, want ErrNilHandler", err)
	}
	var nilStruct *nilQueryHandler
	if _, err := query.Wrap[testProfileQuery, testProfileResult](nilStruct); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("typed nil struct got %v, want ErrNilHandler", err)
	}

	var trace []string
	wrapped, err := query.Wrap(
		query.Func[testProfileQuery, testProfileResult](func(_ context.Context, q testProfileQuery) (testProfileResult, error) {
			trace = append(trace, "handler")
			return testProfileResult{Name: "Ada:" + q.ID}, nil
		}),
		func(next query.UntypedFunc) query.UntypedFunc {
			return func(ctx context.Context, q any) (any, error) {
				trace = append(trace, "mw1")
				return next(ctx, q)
			}
		},
		nil,
		func(next query.UntypedFunc) query.UntypedFunc {
			return func(ctx context.Context, q any) (any, error) {
				trace = append(trace, "mw2")
				return next(ctx, q)
			}
		},
	)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	res, handleErr := wrapped.Handle(context.Background(), testProfileQuery{ID: "42"})
	if handleErr != nil {
		t.Fatalf("Handle: %v", handleErr)
	}
	if res.Name != "Ada:42" || !slices.Equal(trace, []string{"mw1", "mw2", "handler"}) {
		t.Fatalf("unexpected res=%+v trace=%v", res, trace)
	}
}

func TestBus_ExecuteHappyAndFailPaths(t *testing.T) {
	t.Parallel()

	var nilBus *query.Bus
	if err := query.Register(nilBus, query.Func[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("got %v, want ErrNilBus", err)
	}
	if _, err := query.Execute[testProfileResult](context.Background(), nilBus, testProfileQuery{}); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("nilBus Execute got %v, want ErrNilBus", err)
	}

	errRead := errors.New("read replica down")
	var mwOrder []string
	bus, err := query.NewBus(
		nil,
		query.WithMiddleware(func(next query.UntypedFunc) query.UntypedFunc {
			return func(ctx context.Context, q any) (any, error) {
				mwOrder = append(mwOrder, "bus")
				return next(ctx, q)
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	if err := query.Register[testProfileQuery, testProfileResult](bus, nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("nil handler got %v, want ErrNilHandler", err)
	}
	if err := query.Register(bus, query.Func[testQueryInterface, testProfileResult](
		func(context.Context, testQueryInterface) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrInvalidMessageType) {
		t.Fatalf("interface query type got %v, want ErrInvalidMessageType", err)
	}

	query.MustRegister(bus, query.Func[testProfileQuery, testProfileResult](
		func(_ context.Context, q testProfileQuery) (testProfileResult, error) {
			mwOrder = append(mwOrder, "handler")
			if q.ID == "fail" {
				return testProfileResult{}, errRead
			}
			return testProfileResult{Name: "guest-" + q.ID}, nil
		},
	), func(next query.UntypedFunc) query.UntypedFunc {
		return func(ctx context.Context, q any) (any, error) {
			mwOrder = append(mwOrder, "route")
			return next(ctx, q)
		}
	})

	if err := query.Register(bus, query.Func[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrDuplicateHandler) {
		t.Fatalf("duplicate Register got %v, want ErrDuplicateHandler", err)
	}

	// Happy path with Q inferred from argument
	got, qErr := query.Execute[testProfileResult](context.Background(), bus, testProfileQuery{ID: "7"})
	if qErr != nil {
		t.Fatalf("Execute: %v", qErr)
	}
	if got.Name != "guest-7" || !slices.Equal(mwOrder, []string{"bus", "route", "handler"}) {
		t.Fatalf("unexpected query result=%+v mwOrder=%v", got, mwOrder)
	}

	// Handler error passes straight through
	if _, qErr = query.Execute[testProfileResult](context.Background(), bus, testProfileQuery{ID: "fail"}); !errors.Is(qErr, errRead) {
		t.Fatalf("got %v, want %v", qErr, errRead)
	}

	// Unregistered query
	type unknownQuery struct{}
	if _, qErr = query.Execute[testProfileResult](context.Background(), bus, unknownQuery{}); !errors.Is(qErr, cqrs.ErrNoHandler) {
		t.Fatalf("unknown query got %v, want ErrNoHandler", qErr)
	}

	// Registered result type mismatch
	if _, qErr = query.Execute[wrongProfileResult](context.Background(), bus, testProfileQuery{ID: "7"}); !errors.Is(qErr, query.ErrInvalidResultType) {
		t.Fatalf("result type mismatch got %v, want ErrInvalidResultType", qErr)
	}

	// Pre-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, qErr = query.Execute[testProfileResult](ctx, bus, testProfileQuery{ID: "7"}); !errors.Is(qErr, context.Canceled) {
		t.Fatalf("canceled query got %v, want context.Canceled", qErr)
	}

	t.Run("nil ctx fails loudly instead of defaulting to Background", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recover() == nil {
				t.Fatal("expected Execute(nil, ...) to panic")
			}
		}()
		//nolint:staticcheck // SA1012: verifying strict non-nil context contract
		if _, qErr := query.Execute[testProfileResult](nil, bus, testProfileQuery{ID: "7"}); qErr != nil {
			t.Fatalf("expected panic on nil ctx, got err=%v", qErr)
		}
	})
}

func TestBus_PointerQueryAndNilReturnSafety(t *testing.T) {
	t.Parallel()

	type getPtrQuery struct{ ID string }

	bus, err := query.NewBus()
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	query.MustRegister(bus, query.Func[*getPtrQuery, *testProfileResult](
		func(_ context.Context, q *getPtrQuery) (*testProfileResult, error) {
			if q.ID == "missing" {
				return nil, nil
			}
			return &testProfileResult{Name: q.ID}, nil
		},
	))

	// Typed nil query pointer rejected upfront
	var nilQuery *getPtrQuery
	if _, qErr := query.Execute[*testProfileResult](context.Background(), bus, nilQuery); !errors.Is(qErr, query.ErrNilQuery) {
		t.Fatalf("typed nil query got %v, want ErrNilQuery", qErr)
	}

	// Nil pointer result handled safely as typed nil return
	res, qErr := query.Execute[*testProfileResult](context.Background(), bus, &getPtrQuery{ID: "missing"})
	if qErr != nil || res != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", res, qErr)
	}

	// Middleware corrupting return type triggers ErrInvalidResultType
	badBus, badErr := query.NewBus(query.WithMiddleware(func(next query.UntypedFunc) query.UntypedFunc {
		return func(ctx context.Context, q any) (any, error) {
			if _, innerErr := next(ctx, q); innerErr != nil {
				return nil, innerErr
			}
			return "corrupted-string", nil
		}
	}))
	if badErr != nil {
		t.Fatalf("unexpected error building badBus: %v", badErr)
	}
	query.MustRegister(badBus, query.Func[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{Name: "ok"}, nil
		},
	))
	if _, qErr := query.Execute[testProfileResult](context.Background(), badBus, testProfileQuery{}); !errors.Is(qErr, query.ErrInvalidResultType) {
		t.Fatalf("corrupted middleware result got %v, want ErrInvalidResultType", qErr)
	}
}

func TestBus_MustRegisterPanics(t *testing.T) {
	t.Parallel()

	bus, err := query.NewBus()
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected MustRegister to panic on nil handler")
		}
	}()
	query.MustRegister[testProfileQuery, testProfileResult](bus, nil)
}
