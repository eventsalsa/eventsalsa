package domain_test

import (
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/style-2/internal/domain"
)

func TestNewOrder(t *testing.T) {
	t.Parallel()

	order, err := domain.NewOrder("ord-1", "cust-42", 2500)
	if err != nil {
		t.Fatalf("NewOrder unexpected error: %v", err)
	}
	if order.Status != domain.StatusPending || order.TotalCents != 2500 {
		t.Fatalf("unexpected order: %+v", order)
	}

	if _, invalidErr := domain.NewOrder("ord-2", "cust-42", 0); !errors.Is(invalidErr, domain.ErrInvalidOrderTotal) {
		t.Fatalf("zero total err = %v, want %v", invalidErr, domain.ErrInvalidOrderTotal)
	}
}

func TestOrderCancel(t *testing.T) {
	t.Parallel()

	order, err := domain.NewOrder("ord-1", "cust-42", 2500)
	if err != nil {
		t.Fatalf("NewOrder unexpected error: %v", err)
	}

	if blankErr := order.Cancel("   "); !errors.Is(blankErr, domain.ErrCancellationReasonRequired) {
		t.Fatalf("blank reason err = %v, want %v", blankErr, domain.ErrCancellationReasonRequired)
	}
	if order.Status != domain.StatusPending {
		t.Fatalf("status mutated on rejected cancel: %q", order.Status)
	}

	if cancelErr := order.Cancel("customer request"); cancelErr != nil {
		t.Fatalf("Cancel unexpected error: %v", cancelErr)
	}
	if order.Status != domain.StatusCanceled || order.CancelReason != "customer request" {
		t.Fatalf("unexpected canceled order: %+v", order)
	}

	if dupErr := order.Cancel("duplicate"); !errors.Is(dupErr, domain.ErrAlreadyCanceled) {
		t.Fatalf("duplicate cancel err = %v, want %v", dupErr, domain.ErrAlreadyCanceled)
	}
}
