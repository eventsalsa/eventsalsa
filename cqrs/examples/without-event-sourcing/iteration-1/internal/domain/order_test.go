package domain_test

import (
	"errors"
	"testing"

	"eventsalsa.dev/cqrs/examples/without-event-sourcing/iteration-1/internal/domain"
)

func TestNewOrderAndCancelInvariants(t *testing.T) {
	t.Parallel()

	for _, badTotal := range []int64{0, -500} {
		if _, err := domain.NewOrder("ord-bad", "cust-1", badTotal); !errors.Is(err, domain.ErrInvalidOrderTotal) {
			t.Fatalf("NewOrder(%d) err = %v, want %v", badTotal, err, domain.ErrInvalidOrderTotal)
		}
	}

	order, err := domain.NewOrder("ord-1", "cust-1", 1900)
	if err != nil {
		t.Fatalf("NewOrder valid failed: %v", err)
	}
	if order.ID != "ord-1" || order.CustomerID != "cust-1" ||
		order.Status != domain.StatusPending || order.TotalCents != 1900 {
		t.Fatalf("unexpected order fields: %+v", order)
	}

	if err := order.Cancel(); err != nil {
		t.Fatalf("first Cancel failed: %v", err)
	}
	if order.Status != domain.StatusCanceled {
		t.Fatalf("status = %q, want %q", order.Status, domain.StatusCanceled)
	}
	if err := order.Cancel(); !errors.Is(err, domain.ErrAlreadyCanceled) {
		t.Fatalf("duplicate Cancel err = %v, want %v", err, domain.ErrAlreadyCanceled)
	}
}
