package spectest_test

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"eventsalsa.dev/cqrs/spectest"
)

var (
	errOrderAlreadyPlaced = errors.New("order already placed")
	errOrderNotFound      = errors.New("order not found")
	errOrderConfirmed     = errors.New("order already confirmed")
)

type orderPlaced struct {
	CustomerID string
	Currency   string
}

type orderLineAdded struct {
	SKU            string
	Quantity       int
	UnitPriceCents int64
}

type orderConfirmed struct {
	ConfirmedAt time.Time
}

type placeOrder struct {
	CustomerID string
	Currency   string
}

type confirmOrder struct {
	ConfirmedAt time.Time
}

type orderState struct {
	ID         string
	CustomerID string
	Currency   string
	TotalCents int64
	Placed     bool
	Confirmed  bool
}

func initialOrder(id string) orderState {
	return orderState{ID: id}
}

func evolveOrder(s orderState, e any) orderState {
	switch ev := e.(type) {
	case orderPlaced:
		s.CustomerID = ev.CustomerID
		s.Currency = ev.Currency
		s.Placed = true
	case orderLineAdded:
		s.TotalCents += int64(ev.Quantity) * ev.UnitPriceCents
	case orderConfirmed:
		s.Confirmed = true
	}
	return s
}

func decidePlaceOrder(s orderState, cmd placeOrder) ([]any, error) {
	if s.Placed {
		return nil, errOrderAlreadyPlaced
	}
	return []any{orderPlaced(cmd)}, nil
}

func decideConfirmOrder(s orderState, cmd confirmOrder) ([]any, error) {
	if !s.Placed {
		return nil, errOrderNotFound
	}
	if s.Confirmed {
		return nil, nil // idempotent no-op
	}
	if s.TotalCents <= 0 {
		return nil, errOrderConfirmed
	}
	return []any{orderConfirmed(cmd)}, nil
}

func TestSpec_GivenWhenThen(t *testing.T) {
	t.Parallel()

	fixedNow := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	spec := spectest.For(t, initialOrder, evolveOrder)

	// Empty Given via direct Spec.When propagates streamID and supports multi-event emission.
	type placeWithLineCmd struct {
		CustomerID string
		Currency   string
		SKU        string
		Qty        int
		Price      int64
	}
	decidePlaceWithLine := func(_ orderState, cmd placeWithLineCmd) ([]any, error) {
		return []any{
			orderPlaced{CustomerID: cmd.CustomerID, Currency: cmd.Currency},
			orderLineAdded{SKU: cmd.SKU, Quantity: cmd.Qty, UnitPriceCents: cmd.Price},
		}, nil
	}
	spec.When("ord-multi", placeWithLineCmd{
		CustomerID: "cust-1",
		Currency:   "EUR",
		SKU:        "tamper",
		Qty:        3,
		Price:      1500,
	}, decidePlaceWithLine).
		Then(
			orderPlaced{CustomerID: "cust-1", Currency: "EUR"},
			orderLineAdded{SKU: "tamper", Quantity: 3, UnitPriceCents: 1500},
		).
		ThenState(func(s orderState) {
			if s.ID != "ord-multi" || !s.Placed || s.TotalCents != 4500 {
				t.Fatalf("unexpected multi-event state: %+v", s)
			}
		})

	// Empty Given via direct Spec.When
	spec.When("ord-1", placeOrder{CustomerID: "cust-1", Currency: "EUR"}, decidePlaceOrder).
		Then(orderPlaced{CustomerID: "cust-1", Currency: "EUR"}).
		ThenState(func(s orderState) {
			if s.ID != "ord-1" || !s.Placed || s.CustomerID != "cust-1" || s.Currency != "EUR" {
				t.Fatalf("unexpected post-place state: %+v", s)
			}
		})

	// Given + When -> emitted events + state
	spec.Given(
		orderPlaced{CustomerID: "cust-1", Currency: "EUR"},
		orderLineAdded{SKU: "grinder", Quantity: 2, UnitPriceCents: 12900},
	).When("ord-1", confirmOrder{ConfirmedAt: fixedNow}, decideConfirmOrder).
		Then(orderConfirmed{ConfirmedAt: fixedNow}).
		ThenState(func(s orderState) {
			if !s.Confirmed || s.TotalCents != 25800 {
				t.Fatalf("unexpected confirmed state: %+v", s)
			}
		})

	// No-op confirmation when already confirmed
	spec.Given(
		orderPlaced{CustomerID: "cust-1", Currency: "EUR"},
		orderLineAdded{SKU: "grinder", Quantity: 1, UnitPriceCents: 12900},
		orderConfirmed{ConfirmedAt: fixedNow},
	).When("ord-1", confirmOrder{ConfirmedAt: fixedNow}, decideConfirmOrder).
		ThenNoEvents().
		Then()

	// Domain rejection on empty stream
	spec.Given().
		When("ord-1", confirmOrder{ConfirmedAt: fixedNow}, decideConfirmOrder).
		ThenError().
		ThenErrorIs(errOrderNotFound).
		ThenErrorCheck(func(err error) {
			if !errors.Is(err, errOrderNotFound) {
				t.Fatalf("unexpected error in check: %v", err)
			}
		})
}

type recordingTB struct {
	testing.TB
	mu     sync.Mutex
	failed bool
	msg    string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatal(args ...any) {
	r.mu.Lock()
	r.failed = true
	r.msg = fmt.Sprint(args...)
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.failed = true
	r.msg = fmt.Sprintf(format, args...)
	r.mu.Unlock()
	runtime.Goexit()
}

func expectSpecFatal(t *testing.T, fn func(tb testing.TB)) {
	t.Helper()
	rec := &recordingTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(rec)
	}()
	<-done
	if !rec.failed {
		t.Fatal("expected spectest assertion to call Fatal/Fatalf, but it passed")
	}
}

func TestSpec_AssertionFailuresAndGuards(t *testing.T) {
	t.Parallel()

	t.Run("nil TB panics", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatal("expected For(nil, ...) to panic")
			}
		}()
		spectest.For(nil, initialOrder, evolveOrder)
	})

	cases := []struct {
		name string
		run  func(tb testing.TB)
	}{
		{
			name: "nil initial fails",
			run:  func(tb testing.TB) { spectest.For[orderState, any](tb, nil, evolveOrder) },
		},
		{
			name: "nil evolve fails",
			run:  func(tb testing.TB) { spectest.For[orderState, any](tb, initialOrder, nil) },
		},
		{
			name: "nil decide fails",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).When("ord-1", placeOrder{}, nil)
			},
		},
		{
			name: "Then fails when decide returns error",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", confirmOrder{}, decideConfirmOrder).
					Then(orderConfirmed{})
			},
		},
		{
			name: "Then fails when events differ",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{CustomerID: "c-1", Currency: "USD"}, decidePlaceOrder).
					Then(orderPlaced{CustomerID: "other", Currency: "USD"})
			},
		},
		{
			name: "Then() empty fails when events were emitted",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{CustomerID: "c-1", Currency: "USD"}, decidePlaceOrder).
					Then()
			},
		},
		{
			name: "ThenNoEvents fails on error",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", confirmOrder{}, decideConfirmOrder).
					ThenNoEvents()
			},
		},
		{
			name: "ThenNoEvents fails when event emitted",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{CustomerID: "c-1", Currency: "USD"}, decidePlaceOrder).
					ThenNoEvents()
			},
		},
		{
			name: "ThenState fails on error or nil check",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", confirmOrder{}, decideConfirmOrder).
					ThenState(func(orderState) {})
			},
		},
		{
			name: "ThenState nil check fails",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{}, decidePlaceOrder).
					ThenState(nil)
			},
		},
		{
			name: "ThenError fails on success",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{}, decidePlaceOrder).
					ThenError()
			},
		},
		{
			name: "ThenErrorIs fails on success or wrong error",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", confirmOrder{}, decideConfirmOrder).
					ThenErrorIs(errOrderAlreadyPlaced)
			},
		},
		{
			name: "ThenErrorIs fails on success",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{}, decidePlaceOrder).
					ThenErrorIs(errOrderNotFound)
			},
		},
		{
			name: "ThenErrorCheck fails on success or nil func",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", confirmOrder{}, decideConfirmOrder).
					ThenErrorCheck(nil)
			},
		},
		{
			name: "ThenErrorCheck fails on success",
			run: func(tb testing.TB) {
				spectest.For(tb, initialOrder, evolveOrder).
					When("ord-1", placeOrder{}, decidePlaceOrder).
					ThenErrorCheck(func(error) {})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expectSpecFatal(t, tc.run)
		})
	}
}
