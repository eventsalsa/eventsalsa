package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
)

type spyTx struct {
	pgx.Tx
	db                  *spyDB
	id                  int
	txOpts              pgx.TxOptions
	committed           bool
	rolledBack          bool
	rollbackCtxErr      error
	rollbackDeadline    time.Time
	hasRollbackDeadline bool
	commitErr           error
	rollbackErr         error
}

func (tx *spyTx) Commit(_ context.Context) error {
	tx.db.recordTimeline(tx.label() + ":commit")
	if tx.commitErr != nil {
		return tx.commitErr
	}
	if tx.rolledBack || tx.committed {
		return pgx.ErrTxClosed
	}
	tx.committed = true
	return nil
}

func (tx *spyTx) Rollback(ctx context.Context) error {
	tx.rollbackCtxErr = ctx.Err()
	tx.rollbackDeadline, tx.hasRollbackDeadline = ctx.Deadline()
	tx.db.recordTimeline(tx.label() + ":rollback")
	if tx.rollbackErr != nil {
		return tx.rollbackErr
	}
	if tx.committed || tx.rolledBack {
		return pgx.ErrTxClosed
	}
	tx.rolledBack = true
	return nil
}

func (tx *spyTx) label() string {
	if tx.txOpts.AccessMode == pgx.ReadOnly {
		return "read_tx"
	}
	return "write_tx"
}

type spyDB struct {
	mu           sync.Mutex
	beginErr     error
	returnNilTx  bool
	typedNilTx   bool
	nextCommit   error
	nextRollback error
	txs          []*spyTx
	timeline     []string
}

func (db *spyDB) BeginTx(_ context.Context, opts pgx.TxOptions) (pgx.Tx, error) { //nolint:gocritic // hugeParam: implements postgres.TxBeginner
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.beginErr != nil {
		return nil, db.beginErr
	}
	if db.returnNilTx {
		return nil, nil
	}
	if db.typedNilTx {
		var nilSpy *spyTx
		return nilSpy, nil
	}

	tx := &spyTx{
		db:          db,
		id:          len(db.txs) + 1,
		txOpts:      opts,
		commitErr:   db.nextCommit,
		rollbackErr: db.nextRollback,
	}
	db.nextCommit = nil
	db.nextRollback = nil
	db.txs = append(db.txs, tx)
	db.timeline = append(db.timeline, tx.label()+":begin")
	return tx, nil
}

func (db *spyDB) recordTimeline(entry string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.timeline = append(db.timeline, entry)
}

func (db *spyDB) snapshotTimeline() []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	out := make([]string, len(db.timeline))
	copy(out, db.timeline)
	return out
}

func (db *spyDB) txCount() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return len(db.txs)
}

func (db *spyDB) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unused")
}

type orderAdded struct {
	Delta int `json:"delta"`
}

type accountState struct {
	ID      string `json:"id"`
	Balance int    `json:"balance"`
	Version int64  `json:"version"`
}

func accountDecider() persistence.Decider[accountState, any] {
	return persistence.Decider[accountState, any]{
		Initial: func(id string) accountState {
			return accountState{ID: id}
		},
		Evolve: func(s accountState, ev any) accountState {
			s.Version++
			if add, ok := ev.(orderAdded); ok {
				s.Balance += add.Delta
			}
			return s
		},
	}
}

func jsonAccountCodec() postgres.EventCodec[any] {
	return postgres.FuncEventCodec[any]{
		EncodeFunc: func(_ context.Context, streamType, streamID string, ev any) (store.Event, error) {
			raw, err := json.Marshal(ev)
			if err != nil {
				return store.Event{}, err
			}
			return store.Event{
				StreamType:   streamType,
				StreamID:     streamID,
				EventType:    "OrderAdded",
				EventVersion: 1,
				Payload:      raw,
			}, nil
		},
		DecodeFunc: func(_ context.Context, pe store.PersistedEvent) (any, error) {
			var out orderAdded
			if err := json.Unmarshal(pe.Payload, &out); err != nil {
				return nil, err
			}
			return out, nil
		},
	}
}

type spyStore struct {
	mu           sync.Mutex
	db           *spyDB
	streams      map[string][]store.PersistedEvent
	readErr      error
	appendErr    error
	appendCalls  int
	lastExpected store.ExpectedVersion
	beforeAppend func(tx pgx.Tx, expected store.ExpectedVersion, events []store.Event) error
	failOnceOCC  int
}

func newSpyStore(db *spyDB) *spyStore {
	return &spyStore{
		db:      db,
		streams: make(map[string][]store.PersistedEvent),
	}
}

func streamKey(streamType, streamID string) string {
	return streamType + "::" + streamID
}

func (s *spyStore) ReadStream(
	_ context.Context,
	_ pgx.Tx,
	streamType, streamID string,
	fromVersion, toVersion *int64,
) (store.Stream, error) {
	if s.db != nil {
		s.db.recordTimeline("store:read")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readErr != nil {
		return store.Stream{}, s.readErr
	}

	all := s.streams[streamKey(streamType, streamID)]
	var filtered []store.PersistedEvent
	for i := range all {
		v := all[i].StreamVersion
		if fromVersion != nil && v < *fromVersion {
			continue
		}
		if toVersion != nil && v > *toVersion {
			continue
		}
		filtered = append(filtered, all[i])
	}
	return store.Stream{
		StreamType: streamType,
		StreamID:   streamID,
		Events:     filtered,
	}, nil
}

func (s *spyStore) Append(
	_ context.Context,
	tx pgx.Tx,
	expectedVersion store.ExpectedVersion,
	events []store.Event,
) (store.AppendResult, error) {
	if s.db != nil {
		s.db.recordTimeline("store:append")
	}

	s.mu.Lock()
	before := s.beforeAppend
	s.mu.Unlock()
	if before != nil {
		if err := before(tx, expectedVersion, events); err != nil {
			return store.AppendResult{}, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.appendCalls++
	s.lastExpected = expectedVersion

	if s.failOnceOCC > 0 {
		s.failOnceOCC--
		return store.AppendResult{}, store.ErrOptimisticConcurrency
	}
	if s.appendErr != nil {
		return store.AppendResult{}, s.appendErr
	}

	key := streamKey(events[0].StreamType, events[0].StreamID)
	existing := s.streams[key]
	curVer := int64(len(existing))
	if !expectedVersion.Matches(curVer) {
		return store.AppendResult{}, store.ErrOptimisticConcurrency
	}

	persisted := make([]store.PersistedEvent, len(events))
	positions := make([]int64, len(events))
	for i := range events {
		ver := curVer + 1 + int64(i)
		pos := ver * 10
		eid := events[i].EventID
		if eid == uuid.Nil {
			eid = uuid.New()
		}
		persisted[i] = store.PersistedEvent{
			GlobalPosition: pos,
			TransactionID:  uint64(pos),
			StreamType:     events[i].StreamType,
			StreamID:       events[i].StreamID,
			StreamVersion:  ver,
			EventID:        eid,
			EventType:      events[i].EventType,
			EventVersion:   events[i].EventVersion,
			Payload:        events[i].Payload,
			Metadata:       events[i].Metadata,
			TraceID:        events[i].TraceID,
			CausationID:    events[i].CausationID,
			CreatedAt:      events[i].CreatedAt,
		}
		positions[i] = pos
	}
	s.streams[key] = append(existing, persisted...)
	return store.AppendResult{Events: persisted, GlobalPositions: positions}, nil
}

type splitEventStore struct {
	es store.EventStore
}

func (w splitEventStore) Append(
	ctx context.Context,
	tx pgx.Tx,
	expectedVersion store.ExpectedVersion,
	events []store.Event,
) (store.AppendResult, error) {
	return w.es.Append(ctx, tx, expectedVersion, events)
}
