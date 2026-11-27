package domain_test

import (
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/spectest"
)

type placeSpec struct {
	customerID string
	totalCents int64
}

type confirmSpec struct{}

func decidePlaceSpec(s domain.OrderState, in placeSpec) ([]any, error) {
	return domain.Place(s, in.customerID, in.totalCents)
}

func decideConfirmSpec(s domain.OrderState, _ confirmSpec) ([]any, error) {
	return domain.Confirm(s)
}

func TestOrderDomainSpecs(t *testing.T) {
	t.Parallel()

	spec := spectest.For(t, domain.InitialOrder, domain.EvolveOrder)

	spec.When("ord-1", placeSpec{customerID: "cust-42", totalCents: 12900}, decidePlaceSpec).
		Then(domain.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900}).
		ThenState(func(s domain.OrderState) {
			if !s.Placed || s.TotalCents != 12900 {
				t.Fatalf("unexpected state: %+v", s)
			}
		})

	spec.When("ord-1", placeSpec{customerID: "cust-42", totalCents: 0}, decidePlaceSpec).
		ThenErrorIs(domain.ErrInvalidAmount)

	spec.Given(
		domain.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900},
		domain.OrderConfirmed{},
	).When("ord-1", confirmSpec{}, decideConfirmSpec).
		ThenNoEvents()

	spec.Given().
		When("ord-1", confirmSpec{}, decideConfirmSpec).
		ThenErrorIs(domain.ErrNotPlaced)
}
