//go:build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
	storepostgres "eventsalsa.dev/store/postgres"
)

func TestConcurrentOCCRetryStorm(t *testing.T) {
	pool := setupPool(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         pool,
		EventStore: es,
		StreamType: "Storm",
		Codec:      codec,
		Projections: []postgres.Projection{
			accountBalanceProjection(codec),
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	h := persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, accountDecider()),
		func(c creditCmd) string { return c.id },
		func(_ accountState, c creditCmd) ([]any, error) {
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	)

	bus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 25,
				BaseBackoff: 2 * time.Millisecond,
				MaxBackoff:  25 * time.Millisecond,
				RetryIf:     postgres.IsConcurrencyConflict,
			}),
		),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, h)

	const workers = 10
	streamID := "storm-" + uuid.NewString()
	errs := make(chan error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(amount int) {
			defer wg.Done()
			<-start
			errs <- bus.Dispatch(ctx, creditCmd{id: streamID, amount: amount})
		}(10)
	}
	close(start)
	wg.Wait()
	close(errs)

	for dispatchErr := range errs {
		if dispatchErr != nil {
			t.Fatalf("concurrent worker failed: %v", dispatchErr)
		}
	}

	events, ver, err := repo.Load(ctx, streamID)
	if err != nil {
		t.Fatalf("Load final stream: %v", err)
	}
	if ver != workers || len(events) != workers {
		t.Fatalf("version=%d len=%d, want %d", ver, len(events), workers)
	}

	var projBal int
	var projVer int64
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM order_overview_v1 WHERE order_id = $1`, streamID).Scan(&projBal, &projVer); err != nil {
		t.Fatalf("query projection: %v", err)
	}
	if projBal != workers*10 || projVer != workers {
		t.Fatalf("projection = (bal=%d, ver=%d), want (%d, %d)", projBal, projVer, workers*10, workers)
	}

	// Verify direct OCC failure reports IsConcurrencyConflict when unretried.
	occErr := repo.Append(ctx, streamID, 1, []any{accountCredited{Amount: 5}})
	if !postgres.IsConcurrencyConflict(occErr) || !errors.Is(occErr, persistence.ErrConcurrencyConflict) || !errors.Is(occErr, store.ErrOptimisticConcurrency) {
		t.Fatalf("expected dual OCC conflict, got: %v", occErr)
	}
}
