package persistence_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/config"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/infra/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/snapshot"
	"eventsalsa.dev/store"
)

type stubTxBeginner struct{}

func (stubTxBeginner) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { //nolint:gocritic // matches postgres.TxBeginner
	return nil, errors.New("unused")
}

type stubStoreAndReader struct{}

func (stubStoreAndReader) ReadStream(
	context.Context,
	pgx.Tx,
	string, string,
	*int64, *int64,
) (store.Stream, error) {
	return store.Stream{}, nil
}

func (stubStoreAndReader) Append(
	context.Context,
	pgx.Tx,
	store.ExpectedVersion,
	[]store.Event,
) (store.AppendResult, error) {
	return store.AppendResult{}, nil
}

type stubSnapshotStore struct{}

func (stubSnapshotStore) Get(context.Context, pgx.Tx, string, string, int) (snapshot.Snapshot, error) {
	return snapshot.Snapshot{}, nil
}

func (stubSnapshotStore) Put(context.Context, pgx.Tx, *snapshot.Snapshot) error {
	return nil
}

func TestNewOrderAdapter(t *testing.T) {
	t.Parallel()

	snapCfg := config.Default().Snapshot
	if _, err := persistence.NewOrderAdapter(nil); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("nil params err = %v, want %v", err, postgres.ErrNilDB)
	}
	if _, err := persistence.NewOrderAdapter(&persistence.OrderAdapterParams{
		EventStore: stubStoreAndReader{},
		SnapStore:  stubSnapshotStore{},
		Snapshot:   snapCfg,
	}); !errors.Is(err, postgres.ErrNilDB) {
		t.Fatalf("missing DB err = %v, want %v", err, postgres.ErrNilDB)
	}

	adapter, err := persistence.NewOrderAdapter(&persistence.OrderAdapterParams{
		DB:         stubTxBeginner{},
		EventStore: stubStoreAndReader{},
		SnapStore:  stubSnapshotStore{},
		Snapshot:   snapCfg,
	})
	if err != nil {
		t.Fatalf("NewOrderAdapter valid err = %v", err)
	}
	_ = adapter

	noCompress := snapCfg
	noCompress.Compress = false
	if _, err := persistence.NewOrderAdapter(&persistence.OrderAdapterParams{
		DB:         stubTxBeginner{},
		EventStore: stubStoreAndReader{},
		SnapStore:  stubSnapshotStore{},
		Snapshot:   noCompress,
	}); err != nil {
		t.Fatalf("NewOrderAdapter uncompressed err = %v", err)
	}
}
