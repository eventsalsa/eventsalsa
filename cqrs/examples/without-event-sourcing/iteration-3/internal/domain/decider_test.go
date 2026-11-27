package domain_test

import (
	"testing"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-3/internal/domain"
	"eventsalsa.dev/cqrs/spectest"
)

func TestOrderDeciderSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, domain.InitialOrder, domain.EvolveOrder)

	spec.When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}, domain.DecidePlaceOrder).
		Then(domain.OrderPlaced{
			OrderID:    "ord-1",
			CustomerID: "cust-42",
			TotalCents: 2500,
		}).
		ThenState(func(s domain.Order) {
			if s.Status != domain.StatusPending || s.TotalCents != 2500 {
				t.Fatalf("unexpected state after PlaceOrder: %+v", s)
			}
		})

	spec.When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 0,
	}, domain.DecidePlaceOrder).
		ThenErrorIs(domain.ErrInvalidOrderTotal)

	spec.Given(domain.OrderPlaced{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}).When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}, domain.DecidePlaceOrder).
		ThenErrorIs(domain.ErrAlreadyPlaced)

	spec.When("ord-1", domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "before placement",
	}, domain.DecideCancelOrder).
		ThenErrorIs(domain.ErrNotPlaced)

	spec.Given(domain.OrderPlaced{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}).When("ord-1", domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "   ",
	}, domain.DecideCancelOrder).
		ThenErrorIs(domain.ErrCancellationReasonRequired)

	spec.Given(domain.OrderPlaced{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	}).When("ord-1", domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "customer request",
	}, domain.DecideCancelOrder).
		Then(domain.OrderCanceled{Reason: "customer request"}).
		ThenState(func(s domain.Order) {
			if s.Status != domain.StatusCanceled {
				t.Fatalf("unexpected state after CancelOrder: %+v", s)
			}
		})

	spec.Given(
		domain.OrderPlaced{OrderID: "ord-1", CustomerID: "cust-42", TotalCents: 2500},
		domain.OrderCanceled{Reason: "customer request"},
	).When("ord-1", domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "duplicate",
	}, domain.DecideCancelOrder).
		ThenErrorIs(domain.ErrAlreadyCanceled)
}
