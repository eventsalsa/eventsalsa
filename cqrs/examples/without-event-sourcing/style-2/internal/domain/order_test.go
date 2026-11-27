package domain_test

import (
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

func TestHandlePlaceOrder(t *testing.T) {
	t.Parallel()

	order, err := domain.HandlePlaceOrder(domain.PlaceOrder{
		OrderID:    "ord-1",
		CustomerID: "cust-42",
		TotalCents: 2500,
	})
	if err != nil {
		t.Fatalf("HandlePlaceOrder unexpected error: %v", err)
	}
	if order.Status != domain.StatusPending || order.TotalCents != 2500 {
		t.Fatalf("unexpected order: %+v", order)
	}

	if _, invalidErr := domain.HandlePlaceOrder(domain.PlaceOrder{
		OrderID:    "ord-2",
		CustomerID: "cust-42",
		TotalCents: 0,
	}); !errors.Is(invalidErr, domain.ErrInvalidOrderTotal) {
		t.Fatalf("zero total err = %v, want %v", invalidErr, domain.ErrInvalidOrderTotal)
	}
}

func TestHandleCancelOrder(t *testing.T) {
	t.Parallel()

	initial := domain.Order{
		ID:         "ord-1",
		CustomerID: "cust-42",
		Status:     domain.StatusPending,
		TotalCents: 2500,
	}

	if _, blankErr := domain.HandleCancelOrder(initial, domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "   ",
	}); !errors.Is(blankErr, domain.ErrCancellationReasonRequired) {
		t.Fatalf("blank reason err = %v, want %v", blankErr, domain.ErrCancellationReasonRequired)
	}

	canceled, err := domain.HandleCancelOrder(initial, domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "customer request",
	})
	if err != nil {
		t.Fatalf("HandleCancelOrder unexpected error: %v", err)
	}
	if canceled.Status != domain.StatusCanceled || canceled.CancelReason != "customer request" {
		t.Fatalf("unexpected canceled order: %+v", canceled)
	}
	// Verify caller's value wasn't mutated and repeat cancellation is rejected.
	if initial.Status != domain.StatusPending {
		t.Fatalf("initial order mutated: %+v", initial)
	}
	if _, dupErr := domain.HandleCancelOrder(canceled, domain.CancelOrder{
		OrderID: "ord-1",
		Reason:  "duplicate",
	}); !errors.Is(dupErr, domain.ErrAlreadyCanceled) {
		t.Fatalf("duplicate cancel err = %v, want %v", dupErr, domain.ErrAlreadyCanceled)
	}
}
