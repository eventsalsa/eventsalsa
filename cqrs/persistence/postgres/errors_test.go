package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
)

func TestIsConcurrencyConflict(t *testing.T) {
	t.Parallel()

	db := &spyDB{}
	s := newSpyStore(db)
	s.appendErr = store.ErrOptimisticConcurrency

	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         db,
		EventStore: s,
		StreamType: "Order",
		Codec:      jsonAccountCodec(),
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	err = repo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 5}})
	if !errors.Is(err, persistence.ErrConcurrencyConflict) {
		t.Fatalf("expected persistence.ErrConcurrencyConflict, got: %v", err)
	}
	if !errors.Is(err, store.ErrOptimisticConcurrency) {
		t.Fatalf("expected store.ErrOptimisticConcurrency in error chain, got: %v", err)
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error string")
	}
	if !postgres.IsConcurrencyConflict(err) {
		t.Fatalf("expected IsConcurrencyConflict(err) == true, got false")
	}

	// Already-wrapped persistence.ErrConcurrencyConflict passes through unchanged.
	s.appendErr = fmt.Errorf("%w: %w", persistence.ErrConcurrencyConflict, store.ErrOptimisticConcurrency)
	errAlready := repo.Append(context.Background(), "ord-1", 0, []any{orderAdded{Delta: 5}})
	if !postgres.IsConcurrencyConflict(errAlready) {
		t.Fatalf("expected IsConcurrencyConflict == true for pre-wrapped error, got: %v", errAlready)
	}

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "unrelated", err: errors.New("boom"), want: false},
		{name: "persistence sentinel", err: fmt.Errorf("wrap: %w", persistence.ErrConcurrencyConflict), want: true},
		{name: "store sentinel", err: fmt.Errorf("wrap: %w", store.ErrOptimisticConcurrency), want: true},
	} {
		if got := postgres.IsConcurrencyConflict(tc.err); got != tc.want {
			t.Fatalf("%s: IsConcurrencyConflict = %v, want %v", tc.name, got, tc.want)
		}
	}
}
