package spectest_test

import (
	"errors"
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

	// Empty Given via direct Spec.When
	spec.When("ord-1", placeOrder{CustomerID: "cust-1", Currency: "EUR"}, decidePlaceOrder).
		Then(orderPlaced{CustomerID: "cust-1", Currency: "EUR"}).
		ThenState(func(s orderState) {
			if !s.Placed || s.CustomerID != "cust-1" || s.Currency != "EUR" {
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
