package domain_test

import (
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain/events"
	"eventsalsa.dev/cqrs/spectest"
)

func TestOrderDeciderSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, domain.InitialOrder, domain.EvolveOrder)

	spec.When("ord-1", domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 10000,
	}, domain.DecidePlaceOrder).
		Then(events.OrderPlaced{
			CustomerID: "cust-42",
			TotalCents: 10000,
		}).
		ThenState(func(s domain.Order) {
			if !s.Placed || s.Confirmed || s.Version != 1 || s.TotalCents != 10000 {
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
		TotalCents: 10000,
	}).When("ord-1", domain.AddOrderItem{
		OrderID:   "ord-1",
		SKU:       "sku-salsa",
		UnitCents: 2500,
		Quantity:  2,
	}, domain.DecideAddOrderItem).
		Then(events.OrderItemAdded{
			SKU:       "sku-salsa",
			UnitCents: 2500,
			Quantity:  2,
		}).
		ThenState(func(s domain.Order) {
			if s.Version != 2 || s.ItemCount != 2 || s.TotalCents != 15000 {
				t.Fatalf("unexpected state after AddOrderItem: %+v", s)
			}
		})

	spec.When("ord-1", domain.AddOrderItem{
		OrderID:   "ord-1",
		SKU:       "sku-salsa",
		UnitCents: 2500,
		Quantity:  1,
	}, domain.DecideAddOrderItem).
		ThenErrorIs(domain.ErrNotPlaced)

	spec.Given(events.OrderPlaced{CustomerID: "cust-42", TotalCents: 10000}).
		When("ord-1", domain.AddOrderItem{OrderID: "ord-1", SKU: "  ", UnitCents: 100, Quantity: 1}, domain.DecideAddOrderItem).
		ThenErrorIs(domain.ErrEmptySKU)

	spec.Given(events.OrderPlaced{CustomerID: "cust-42", TotalCents: 10000}).
		When("ord-1", domain.AddOrderItem{OrderID: "ord-1", SKU: "x", UnitCents: 0, Quantity: 1}, domain.DecideAddOrderItem).
		ThenErrorIs(domain.ErrInvalidAmount)

	spec.Given(
		events.OrderPlaced{CustomerID: "cust-42", TotalCents: 10000},
		events.OrderConfirmed{},
	).When("ord-1", domain.AddOrderItem{
		OrderID:   "ord-1",
		SKU:       "sku-late",
		UnitCents: 500,
		Quantity:  1,
	}, domain.DecideAddOrderItem).
		ThenErrorIs(domain.ErrAlreadyConfirmed)

	spec.Given(events.OrderPlaced{
		CustomerID: "cust-42",
		TotalCents: 10000,
	}).When("ord-1", domain.ConfirmOrder{OrderID: "ord-1"}, domain.DecideConfirmOrder).
		Then(events.OrderConfirmed{}).
		ThenState(func(s domain.Order) {
			if !s.Placed || !s.Confirmed || s.Version != 2 {
				t.Fatalf("unexpected state after ConfirmOrder: %+v", s)
			}
		})

	// Second confirmation is an idempotent zero-event no-op.
	spec.Given(
		events.OrderPlaced{CustomerID: "cust-42", TotalCents: 10000},
		events.OrderConfirmed{},
	).When("ord-1", domain.ConfirmOrder{OrderID: "ord-1"}, domain.DecideConfirmOrder).
		Then()
}
