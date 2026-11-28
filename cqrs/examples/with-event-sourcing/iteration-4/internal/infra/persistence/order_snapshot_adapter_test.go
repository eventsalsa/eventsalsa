package persistence_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

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

func TestNewOrderSnapshotAdapter(t *testing.T) {
	t.Parallel()

	if _, err := persistence.NewOrderSnapshotAdapter(nil, stubStoreAndReader{}, stubSnapshotStore{}); !errors.Is(
		err, postgres.ErrNilDB,
	) {
		t.Fatalf("missing DB err = %v, want %v", err, postgres.ErrNilDB)
	}

	adapter, err := persistence.NewOrderSnapshotAdapter(
		stubTxBeginner{},
		stubStoreAndReader{},
		stubSnapshotStore{},
		nil,
		persistence.WithStreamType("CustomOrder"),
		persistence.WithSchemaVersion(2),
		persistence.WithSnapshotPolicy(snapshot.EveryNEvents(5)),
		persistence.WithSnapshotCompression(false),
	)
	if err != nil {
		t.Fatalf("NewOrderSnapshotAdapter with options err = %v", err)
	}
	_ = adapter

	if _, err := persistence.NewOrderSnapshotAdapter(
		stubTxBeginner{},
		stubStoreAndReader{},
		stubSnapshotStore{},
	); err != nil {
		t.Fatalf("NewOrderSnapshotAdapter defaults err = %v", err)
	}
}
