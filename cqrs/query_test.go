package cqrs_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"eventsalsa.dev/cqrs"
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

func TestWrapQueryAndNilGuards(t *testing.T) {
	t.Parallel()

	if _, err := cqrs.WrapQuery[testProfileQuery, testProfileResult](nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("got %v, want ErrNilHandler", err)
	}
	var nilFn cqrs.QueryFunc[testProfileQuery, testProfileResult]
	if _, err := cqrs.WrapQuery[testProfileQuery, testProfileResult](nilFn); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("typed nil func got %v, want ErrNilHandler", err)
	}
	var nilStruct *nilQueryHandler
	if _, err := cqrs.WrapQuery[testProfileQuery, testProfileResult](nilStruct); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("typed nil struct got %v, want ErrNilHandler", err)
	}

	var trace []string
	wrapped, err := cqrs.WrapQuery(
		cqrs.QueryFunc[testProfileQuery, testProfileResult](func(_ context.Context, q testProfileQuery) (testProfileResult, error) {
			trace = append(trace, "handler")
			return testProfileResult{Name: "Ada:" + q.ID}, nil
		}),
		func(next cqrs.UntypedQueryFunc) cqrs.UntypedQueryFunc {
			return func(ctx context.Context, q any) (any, error) {
				trace = append(trace, "mw1")
				return next(ctx, q)
			}
		},
		nil,
		func(next cqrs.UntypedQueryFunc) cqrs.UntypedQueryFunc {
			return func(ctx context.Context, q any) (any, error) {
				trace = append(trace, "mw2")
				return next(ctx, q)
			}
		},
	)
	if err != nil {
		t.Fatalf("WrapQuery: %v", err)
	}

	//nolint:staticcheck // SA1012: intentionally verifying nil context normalization
	res, handleErr := wrapped.Handle(nil, testProfileQuery{ID: "42"})
	if handleErr != nil {
		t.Fatalf("Handle: %v", handleErr)
	}
	if res.Name != "Ada:42" || !slices.Equal(trace, []string{"mw1", "mw2", "handler"}) {
		t.Fatalf("unexpected res=%+v trace=%v", res, trace)
	}
}

func TestQueryBus_HappyAndFailPaths(t *testing.T) {
	t.Parallel()

	var nilBus *cqrs.QueryBus
	if err := cqrs.RegisterQuery(nilBus, cqrs.QueryFunc[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("got %v, want ErrNilBus", err)
	}
	if _, err := cqrs.Query[testProfileResult](context.Background(), nilBus, testProfileQuery{}); !errors.Is(err, cqrs.ErrNilBus) {
		t.Fatalf("nilBus query got %v, want ErrNilBus", err)
	}

	errRead := errors.New("read replica down")
	var mwOrder []string
	bus, err := cqrs.NewQueryBus(
		nil,
		cqrs.WithQueryMiddleware(func(next cqrs.UntypedQueryFunc) cqrs.UntypedQueryFunc {
			return func(ctx context.Context, q any) (any, error) {
				mwOrder = append(mwOrder, "bus")
				return next(ctx, q)
			}
		}),
	)
	if err != nil {
		t.Fatalf("NewQueryBus: %v", err)
	}

	if err := cqrs.RegisterQuery[testProfileQuery, testProfileResult](bus, nil); !errors.Is(err, cqrs.ErrNilHandler) {
		t.Fatalf("nil handler got %v, want ErrNilHandler", err)
	}
	if err := cqrs.RegisterQuery(bus, cqrs.QueryFunc[testQueryInterface, testProfileResult](
		func(context.Context, testQueryInterface) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrInvalidMessageType) {
		t.Fatalf("interface query type got %v, want ErrInvalidMessageType", err)
	}

	cqrs.MustRegisterQuery(bus, cqrs.QueryFunc[testProfileQuery, testProfileResult](
		func(_ context.Context, q testProfileQuery) (testProfileResult, error) {
			mwOrder = append(mwOrder, "handler")
			if q.ID == "fail" {
				return testProfileResult{}, errRead
			}
			return testProfileResult{Name: "guest-" + q.ID}, nil
		},
	), func(next cqrs.UntypedQueryFunc) cqrs.UntypedQueryFunc {
		return func(ctx context.Context, q any) (any, error) {
			mwOrder = append(mwOrder, "route")
			return next(ctx, q)
		}
	})

	if err := cqrs.RegisterQuery(bus, cqrs.QueryFunc[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{}, nil
		},
	)); !errors.Is(err, cqrs.ErrDuplicateHandler) {
		t.Fatalf("duplicate RegisterQuery got %v, want ErrDuplicateHandler", err)
	}

	// Happy path with Q inferred from argument
	got, qErr := cqrs.Query[testProfileResult](context.Background(), bus, testProfileQuery{ID: "7"})
	if qErr != nil {
		t.Fatalf("Query: %v", qErr)
	}
	if got.Name != "guest-7" || !slices.Equal(mwOrder, []string{"bus", "route", "handler"}) {
		t.Fatalf("unexpected query result=%+v mwOrder=%v", got, mwOrder)
	}

	// Handler error passes straight through
	if _, qErr = cqrs.Query[testProfileResult](context.Background(), bus, testProfileQuery{ID: "fail"}); !errors.Is(qErr, errRead) {
		t.Fatalf("got %v, want %v", qErr, errRead)
	}

	// Unregistered query
	type unknownQuery struct{}
	if _, qErr = cqrs.Query[testProfileResult](context.Background(), bus, unknownQuery{}); !errors.Is(qErr, cqrs.ErrNoHandler) {
		t.Fatalf("unknown query got %v, want ErrNoHandler", qErr)
	}

	// Registered result type mismatch
	if _, qErr = cqrs.Query[wrongProfileResult](context.Background(), bus, testProfileQuery{ID: "7"}); !errors.Is(qErr, cqrs.ErrInvalidResultType) {
		t.Fatalf("result type mismatch got %v, want ErrInvalidResultType", qErr)
	}

	// Pre-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, qErr = cqrs.Query[testProfileResult](ctx, bus, testProfileQuery{ID: "7"}); !errors.Is(qErr, context.Canceled) {
		t.Fatalf("canceled query got %v, want context.Canceled", qErr)
	}
}

func TestQueryBus_PointerQueryAndNilReturnSafety(t *testing.T) {
	t.Parallel()

	type getPtrQuery struct{ ID string }

	bus, err := cqrs.NewQueryBus()
	if err != nil {
		t.Fatalf("NewQueryBus: %v", err)
	}
	cqrs.MustRegisterQuery(bus, cqrs.QueryFunc[*getPtrQuery, *testProfileResult](
		func(_ context.Context, q *getPtrQuery) (*testProfileResult, error) {
			if q.ID == "missing" {
				return nil, nil
			}
			return &testProfileResult{Name: q.ID}, nil
		},
	))

	// Typed nil query pointer rejected upfront
	var nilQuery *getPtrQuery
	if _, qErr := cqrs.Query[*testProfileResult](context.Background(), bus, nilQuery); !errors.Is(qErr, cqrs.ErrNilQuery) {
		t.Fatalf("typed nil query got %v, want ErrNilQuery", qErr)
	}

	// Nil pointer result handled safely as typed nil return
	res, qErr := cqrs.Query[*testProfileResult](context.Background(), bus, &getPtrQuery{ID: "missing"})
	if qErr != nil || res != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", res, qErr)
	}

	// Middleware corrupting return type triggers ErrInvalidResultType
	badBus, badErr := cqrs.NewQueryBus(cqrs.WithQueryMiddleware(func(next cqrs.UntypedQueryFunc) cqrs.UntypedQueryFunc {
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
	cqrs.MustRegisterQuery(badBus, cqrs.QueryFunc[testProfileQuery, testProfileResult](
		func(context.Context, testProfileQuery) (testProfileResult, error) {
			return testProfileResult{Name: "ok"}, nil
		},
	))
	if _, qErr := cqrs.Query[testProfileResult](context.Background(), badBus, testProfileQuery{}); !errors.Is(qErr, cqrs.ErrInvalidResultType) {
		t.Fatalf("corrupted middleware result got %v, want ErrInvalidResultType", qErr)
	}
}

func TestQueryBus_MustRegisterPanics(t *testing.T) {
	t.Parallel()

	bus, err := cqrs.NewQueryBus()
	if err != nil {
		t.Fatalf("NewQueryBus: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected MustRegisterQuery panic on nil handler")
		}
	}()
	cqrs.MustRegisterQuery[testProfileQuery, testProfileResult](bus, nil)
}
