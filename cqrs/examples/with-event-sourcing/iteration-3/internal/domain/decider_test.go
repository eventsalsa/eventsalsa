package domain_test

import (
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-3/internal/domain/events"
	"eventsalsa.dev/cqrs/spectest"
)

func TestOrderDeciderSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, domain.InitialOrder, domain.EvolveOrder)

	spec.When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 12900,
	}, domain.DecidePlaceOrder).
		Then(events.OrderPlaced{
			CustomerID: "cust-42",
			TotalCents: 12900,
		}).
		ThenState(func(s domain.Order) {
			if !s.Placed || s.Confirmed || s.CustomerID != "cust-42" || s.TotalCents != 12900 {
				t.Fatalf("unexpected state after PlaceOrder: %+v", s)
			}
		})

	spec.When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 0,
	}, domain.DecidePlaceOrder).
		ThenErrorIs(domain.ErrInvalidAmount)

	spec.Given(events.OrderPlaced{
		CustomerID: "cust-42",
		TotalCents: 12900,
	}).When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 12900,
	}, domain.DecidePlaceOrder).
		ThenErrorIs(domain.ErrAlreadyPlaced)

	spec.When("ord-1", domain.ConfirmOrder{OrderID: "ord-1"}, domain.DecideConfirmOrder).
		ThenErrorIs(domain.ErrNotPlaced)

	spec.Given(events.OrderPlaced{
		CustomerID: "cust-42",
		TotalCents: 12900,
	}).When("ord-1", domain.ConfirmOrder{OrderID: "ord-1"}, domain.DecideConfirmOrder).
		Then(events.OrderConfirmed{}).
		ThenState(func(s domain.Order) {
			if !s.Placed || !s.Confirmed {
				t.Fatalf("unexpected state after ConfirmOrder: %+v", s)
			}
		})

	// Second confirmation is an idempotent zero-event no-op.
	spec.Given(
		events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900},
		events.OrderConfirmed{},
	).When("ord-1", domain.ConfirmOrder{OrderID: "ord-1"}, domain.DecideConfirmOrder).
		Then()
}
