package domain_test

import (
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain/events"
	"eventsalsa.dev/cqrs/spectest"
)

type placeSpec struct {
	customerID string
	totalCents int64
}

type confirmSpec struct{}

func decidePlaceSpec(o domain.Order, in placeSpec) ([]any, error) {
	return domain.Place(o, in.customerID, in.totalCents)
}

func decideConfirmSpec(o domain.Order, _ confirmSpec) ([]any, error) {
	return domain.Confirm(o)
}

func TestOrderDomainSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, domain.InitialOrder, domain.EvolveOrder)

	spec.When("ord-1", placeSpec{customerID: "cust-42", totalCents: 12900}, decidePlaceSpec).
		Then(events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900}).
		ThenState(func(o domain.Order) {
			if !o.Placed || o.TotalCents != 12900 {
				t.Fatalf("unexpected order: %+v", o)
			}
		})

	spec.When("ord-1", placeSpec{customerID: "cust-42", totalCents: 0}, decidePlaceSpec).
		ThenErrorIs(domain.ErrInvalidAmount)

	spec.Given(
		events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900},
		events.OrderConfirmed{},
	).When("ord-1", confirmSpec{}, decideConfirmSpec).
		ThenNoEvents()

	spec.Given().
		When("ord-1", confirmSpec{}, decideConfirmSpec).
		ThenErrorIs(domain.ErrNotPlaced)
}
