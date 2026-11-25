package main

import (
	"testing"

	"eventsalsa.dev/cqrs/spectest"
)

func TestOrderDomainSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, initialOrder, evolveOrder)

	spec.When("ord-1", PlaceOrder{CustomerID: "cust-42", TotalCents: 12900}, decidePlaceOrder).
		Then(OrderPlaced{CustomerID: "cust-42", TotalCents: 12900}).
		ThenState(func(s OrderState) {
			if !s.Placed || s.TotalCents != 12900 {
				t.Fatalf("unexpected state: %+v", s)
			}
		})

	spec.When("ord-1", PlaceOrder{CustomerID: "cust-42", TotalCents: 0}, decidePlaceOrder).
		ThenErrorIs(errInvalidAmount)

	spec.Given(
		OrderPlaced{CustomerID: "cust-42", TotalCents: 12900},
		OrderConfirmed{},
	).When("ord-1", ConfirmOrder{OrderID: "ord-1"}, decideConfirmOrder).
		ThenNoEvents()

	spec.Given().
		When("ord-1", ConfirmOrder{OrderID: "ord-1"}, decideConfirmOrder).
		ThenErrorIs(errNotPlaced)
}
