package postgres

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/store"
)

// EventRepositoryConfig configures a PostgreSQL EventRepository backed by eventsalsa.dev/store.
type EventRepositoryConfig[E any] struct {
	// DB starts narrow pgx.ReadOnly and write transactions when no outer transaction is active in ctx.
	DB TxBeginner

	// EventStore appends domain events atomically (for example *store/postgres.Store).
	EventStore store.EventStore

	// Reader loads event streams. When omitted (nil), EventStore must also implement store.StreamReader.
	Reader store.StreamReader

	// Codec translates between domain events E and store.Event / store.PersistedEvent.
	Codec EventCodec[E]

	// Clock overrides time.Now when filling zero store.Event.CreatedAt timestamps.
	Clock func() time.Time

	// IDGen overrides uuid.New when filling nil store.Event.EventID identifiers.
	IDGen func() uuid.UUID

	// StreamType identifies the aggregate stream partition stored in events.stream_type.
	StreamType string

	// Projections execute synchronously inside the append write transaction for each persisted event.
	Projections []Projection

	// BatchProjections execute synchronously inside the append write transaction for each appended batch.
	BatchProjections []BatchProjection
}

// EventRepository implements eventsalsa.dev/cqrs/persistence.EventRepository[E] over PostgreSQL
// using narrow read/write transactions by default while honoring any active context transaction.
type EventRepository[E any] struct {
	db               TxBeginner
	eventStore       store.EventStore
	reader           store.StreamReader
	codec            EventCodec[E]
	clock            func() time.Time
	idGen            func() uuid.UUID
	streamType       string
	projections      []Projection
	batchProjections []BatchProjection
}

// NewEventRepository validates cfg and returns a configured EventRepository[E].
func NewEventRepository[E any](cfg EventRepositoryConfig[E]) (*EventRepository[E], error) { //nolint:gocritic // hugeParam: value config simplifies caller literals
	if isNilValue(cfg.DB) {
		return nil, ErrNilDB
	}
	if isNilValue(cfg.EventStore) {
		return nil, ErrNilEventStore
	}
	reader, err := resolveStreamReader(cfg.EventStore, cfg.Reader)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.StreamType) == "" {
		return nil, ErrEmptyStreamType
	}
	if err := validateCodec(cfg.Codec); err != nil {
		return nil, err
	}
	if err := validateProjections(cfg.Projections, cfg.BatchProjections); err != nil {
		return nil, err
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	idGen := cfg.IDGen
	if idGen == nil {
		idGen = uuid.New
	}

	return &EventRepository[E]{
		db:               cfg.DB,
		eventStore:       cfg.EventStore,
		reader:           reader,
		streamType:       cfg.StreamType,
		codec:            cfg.Codec,
		projections:      slices.Clone(cfg.Projections),
		batchProjections: slices.Clone(cfg.BatchProjections),
		clock:            clock,
		idGen:            idGen,
	}, nil
}

func resolveStreamReader(es store.EventStore, reader store.StreamReader) (store.StreamReader, error) {
	if isSetTypedNil(reader) {
		return nil, ErrNilStreamReader
	}
	if !isNilValue(reader) {
		return reader, nil
	}
	inferred, ok := es.(store.StreamReader)
	if !ok || isNilValue(inferred) {
		return nil, ErrNilStreamReader
	}
	return inferred, nil
}

func validateCodec[E any](codec EventCodec[E]) error {
	if isNilValue(codec) {
		return ErrNilCodec
	}
	if v, ok := codec.(interface{ validate() error }); ok {
		return v.validate()
	}
	return nil
}

// Load reads and decodes the persisted event stream for id.
//
// When no active pgx.Tx is in ctx, Load runs inside a short pgx.ReadOnly transaction that
// closes before returning so domain decision logic executes with no DB lock held.
func (r *EventRepository[E]) Load(
	ctx context.Context,
	id string,
) (events []E, version int64, err error) {
	if ctx == nil {
		return nil, 0, persistence.ErrNilContext
	}
	if strings.TrimSpace(id) == "" {
		return nil, 0, persistence.ErrEmptyTargetID
	}

	err = InReadTx(ctx, r.db, func(txCtx context.Context, tx pgx.Tx) error {
		var readErr error
		events, version, readErr = r.readEventsInTx(txCtx, tx, id)
		return readErr
	})
	if err != nil {
		return nil, 0, err
	}
	return events, version, nil
}

// Append encodes domain events, stamps missing stream metadata and cqrs observability fields,
// appends optimistically at expectedVersion, and runs inline projections inside a short write
// transaction (or reuses the active context transaction if present).
func (r *EventRepository[E]) Append(ctx context.Context, id string, expectedVersion int64, events []E) error {
	if ctx == nil {
		return persistence.ErrNilContext
	}
	if strings.TrimSpace(id) == "" {
		return persistence.ErrEmptyTargetID
	}
	if expectedVersion < 0 {
		return persistence.ErrInvalidVersion
	}
	if len(events) == 0 {
		return nil
	}

	storeEvents, err := r.encodeEvents(ctx, id, events)
	if err != nil {
		return err
	}

	return InTx(ctx, r.db, func(txCtx context.Context, tx pgx.Tx) error {
		return r.appendAndProjectInTx(txCtx, tx, expectedVersion, storeEvents)
	})
}

func (r *EventRepository[E]) readEventsInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (events []E, version int64, err error) {
	stream, err := r.reader.ReadStream(ctx, tx, r.streamType, id, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	if stream.IsEmpty() {
		return nil, 0, nil
	}
	decoded := make([]E, len(stream.Events))
	for i := range stream.Events {
		ev, decErr := r.codec.Decode(ctx, stream.Events[i])
		if decErr != nil {
			return nil, 0, decErr
		}
		decoded[i] = ev
	}
	return decoded, stream.Version(), nil
}

func (r *EventRepository[E]) encodeEvents(ctx context.Context, id string, events []E) ([]store.Event, error) {
	obs, _ := cqrs.ObservabilityFromContext(ctx)
	causationID := firstNonEmpty(obs.CommandID, obs.CausationID)

	encoded := make([]store.Event, len(events))
	for i := range events {
		se, err := r.codec.Encode(ctx, r.streamType, id, events[i])
		if err != nil {
			return nil, err
		}
		if err := r.normalizeEvent(&se, id, obs.TraceID, causationID); err != nil {
			return nil, err
		}
		encoded[i] = se
	}
	return encoded, nil
}

func (r *EventRepository[E]) normalizeEvent(
	se *store.Event,
	id string,
	traceID string,
	causationID string,
) error {
	if se.StreamType == "" {
		se.StreamType = r.streamType
	} else if se.StreamType != r.streamType {
		return ErrStreamMismatch
	}
	if se.StreamID == "" {
		se.StreamID = id
	} else if se.StreamID != id {
		return ErrStreamMismatch
	}
	if se.EventID == uuid.Nil {
		se.EventID = r.idGen()
	}
	if se.EventVersion <= 0 {
		se.EventVersion = 1
	}
	if se.CreatedAt.IsZero() {
		se.CreatedAt = r.clock()
	}
	if !se.TraceID.Valid && traceID != "" {
		se.TraceID = store.NullString{String: traceID, Valid: true}
	}
	if !se.CausationID.Valid && causationID != "" {
		se.CausationID = store.NullString{String: causationID, Valid: true}
	}
	return nil
}

func (r *EventRepository[E]) appendAndProjectInTx(
	ctx context.Context,
	tx pgx.Tx,
	expectedVersion int64,
	storeEvents []store.Event,
) error {
	res, err := r.eventStore.Append(ctx, tx, store.Exact(expectedVersion), storeEvents)
	if err != nil {
		if errors.Is(err, store.ErrOptimisticConcurrency) {
			return wrapConcurrencyConflict(err)
		}
		return err
	}

	return r.runProjections(ctx, tx, res.Events)
}

func (r *EventRepository[E]) runProjections(ctx context.Context, tx pgx.Tx, persisted []store.PersistedEvent) error {
	if len(persisted) == 0 {
		return nil
	}
	for _, bp := range r.batchProjections {
		if err := bp.Handle(ctx, tx, persisted); err != nil {
			return err
		}
	}
	for _, p := range r.projections {
		for i := range persisted {
			if err := p.Handle(ctx, tx, persisted[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
