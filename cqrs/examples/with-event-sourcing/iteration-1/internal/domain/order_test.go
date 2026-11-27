package domain_test

import (
	"errors"
	"reflect"
	"testing"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-1/internal/domain/events"
)

func TestPlaceOrder(t *testing.T) {
	t.Parallel()

	order, err := domain.PlaceOrder("ord-1", "cust-42", 12900)
	if err != nil {
		t.Fatalf("PlaceOrder unexpected error: %v", err)
	}
	if !order.Placed || order.TotalCents != 12900 || order.CustomerID != "cust-42" {
		t.Fatalf("unexpected order state: %+v", order)
	}
	if order.Version() != 0 {
		t.Fatalf("Version() = %d, want 0", order.Version())
	}
	wantEvents := []any{events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900}}
	if !reflect.DeepEqual(order.UncommittedEvents(), wantEvents) {
		t.Fatalf("UncommittedEvents() = %#v, want %#v", order.UncommittedEvents(), wantEvents)
	}

	if _, invalidErr := domain.PlaceOrder("ord-2", "cust-42", 0); !errors.Is(invalidErr, domain.ErrInvalidAmount) {
		t.Fatalf("zero total err = %v, want %v", invalidErr, domain.ErrInvalidAmount)
	}
}

func TestOrderReconstituteAndConfirm(t *testing.T) {
	t.Parallel()

	unplaced := domain.ReconstituteOrder("ord-0", 0)
	if notPlacedErr := unplaced.Confirm(); !errors.Is(notPlacedErr, domain.ErrNotPlaced) {
		t.Fatalf("Confirm on unplaced err = %v, want %v", notPlacedErr, domain.ErrNotPlaced)
	}

	order := domain.ReconstituteOrder("ord-1", 1)
	order.Evolve(events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900})

	if order.Version() != 1 {
		t.Fatalf("Version() = %d, want 1", order.Version())
	}
	if len(order.UncommittedEvents()) != 0 {
		t.Fatalf("Evolve should not record uncommitted events, got %#v", order.UncommittedEvents())
	}

	if confirmErr := order.Confirm(); confirmErr != nil {
		t.Fatalf("Confirm unexpected error: %v", confirmErr)
	}
	if !order.Confirmed {
		t.Fatal("expected Confirmed == true")
	}
	wantEvents := []any{events.OrderConfirmed{}}
	if !reflect.DeepEqual(order.UncommittedEvents(), wantEvents) {
		t.Fatalf("UncommittedEvents() = %#v, want %#v", order.UncommittedEvents(), wantEvents)
	}

	// Idempotent confirmation on already confirmed reconstituted aggregate records no events.
	confirmed := domain.ReconstituteOrder("ord-1", 2)
	confirmed.Evolve(events.OrderPlaced{CustomerID: "cust-42", TotalCents: 12900})
	confirmed.Evolve(events.OrderConfirmed{})
	if idempotentErr := confirmed.Confirm(); idempotentErr != nil {
		t.Fatalf("idempotent Confirm unexpected error: %v", idempotentErr)
	}
	if len(confirmed.UncommittedEvents()) != 0 {
		t.Fatalf("expected 0 uncommitted events on idempotent Confirm, got %#v", confirmed.UncommittedEvents())
	}
}
