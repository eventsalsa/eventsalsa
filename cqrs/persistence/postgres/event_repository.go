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

// EventRepositoryConfig configures a narrow-transaction PostgreSQL EventRepository backed by
// eventsalsa.dev/store. Each Load and Append call starts and closes its own transaction via DB
// and rejects calls made inside an active context transaction (ErrTxAlreadyActive).
type EventRepositoryConfig[E any] struct {
	// DB starts narrow pgx.ReadOnly and write transactions for Load and Append. Required.
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

// TxEventRepositoryConfig configures a context-transaction TxEventRepository for use with
// wide transaction middleware (NewTxMiddleware) or AdoptTx. It never starts, commits, or rolls
// back transactions on its own and returns ErrMissingTx when no active transaction is in ctx.
type TxEventRepositoryConfig[E any] struct {
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

	// Projections execute synchronously inside the active transaction for each persisted event.
	Projections []Projection

	// BatchProjections execute synchronously inside the active transaction for each appended batch.
	BatchProjections []BatchProjection
}

type eventRepoCore[E any] struct {
	eventStore       store.EventStore
	reader           store.StreamReader
	codec            EventCodec[E]
	clock            func() time.Time
	idGen            func() uuid.UUID
	streamType       string
	projections      []Projection
	batchProjections []BatchProjection
}

func newEventRepoCore[E any](cfg TxEventRepositoryConfig[E]) (*eventRepoCore[E], error) { //nolint:gocritic // hugeParam: internal builder
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

	return &eventRepoCore[E]{
		eventStore:       cfg.EventStore,
		reader:           reader,
		codec:            cfg.Codec,
		clock:            clock,
		idGen:            idGen,
		streamType:       cfg.StreamType,
		projections:      slices.Clone(cfg.Projections),
		batchProjections: slices.Clone(cfg.BatchProjections),
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

// EventRepository implements eventsalsa.dev/cqrs/persistence.EventRepository[E] using
// dedicated narrow PostgreSQL transactions for Load (pgx.ReadOnly) and Append (read-write).
//
// Calling Load or Append when a transaction is already active in ctx fails with
// ErrTxAlreadyActive. For explicit wide-transaction composition, use LoadTx / AppendTx or
// construct a TxEventRepository via NewTxEventRepository.
type EventRepository[E any] struct {
	db   TxBeginner
	core *eventRepoCore[E]
}

// NewEventRepository validates cfg and returns a narrow-transaction EventRepository[E].
func NewEventRepository[E any](cfg EventRepositoryConfig[E]) (*EventRepository[E], error) { //nolint:gocritic // hugeParam: value config simplifies caller literals
	if isNilValue(cfg.DB) {
		return nil, ErrNilDB
	}
	core, err := newEventRepoCore(TxEventRepositoryConfig[E]{
		EventStore:       cfg.EventStore,
		Reader:           cfg.Reader,
		Codec:            cfg.Codec,
		Clock:            cfg.Clock,
		IDGen:            cfg.IDGen,
		StreamType:       cfg.StreamType,
		Projections:      cfg.Projections,
		BatchProjections: cfg.BatchProjections,
	})
	if err != nil {
		return nil, err
	}
	return &EventRepository[E]{
		db:   cfg.DB,
		core: core,
	}, nil
}

// Load reads and decodes the persisted event stream for id inside a short pgx.ReadOnly
// transaction that commits and releases the connection before returning. Returns
// ErrTxAlreadyActive if ctx already carries an active transaction.
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
		events, version, readErr = r.core.loadTx(txCtx, tx, id)
		return readErr
	})
	if err != nil {
		return nil, 0, err
	}
	return events, version, nil
}

// Append encodes domain events, stamps missing stream metadata and cqrs observability fields,
// and appends optimistically at expectedVersion plus inline projections inside a short write
// transaction. Returns ErrTxAlreadyActive if ctx already carries an active transaction.
func (r *EventRepository[E]) Append(ctx context.Context, id string, expectedVersion int64, events []E) error {
	if ctx == nil {
		return persistence.ErrNilContext
	}
	if _, ok := TxFromContext(ctx); ok {
		return ErrTxAlreadyActive
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

	storeEvents, err := r.core.encodeEvents(ctx, id, events)
	if err != nil {
		return err
	}

	return InTx(ctx, r.db, func(txCtx context.Context, tx pgx.Tx) error {
		return r.core.appendEncodedInTx(txCtx, tx, expectedVersion, storeEvents)
	})
}

// LoadTx reads and decodes the persisted event stream for id using caller-managed tx.
func (r *EventRepository[E]) LoadTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (events []E, version int64, err error) {
	return r.core.loadTx(ctx, tx, id)
}

// AppendTx encodes domain events and appends them with inline projections using caller-managed tx.
func (r *EventRepository[E]) AppendTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	expectedVersion int64,
	events []E,
) error {
	return r.core.appendTx(ctx, tx, id, expectedVersion, events)
}

// TxEventRepository implements eventsalsa.dev/cqrs/persistence.EventRepository[E] using an
// active transaction required from context (started via NewTxMiddleware or AdoptTx). It never
// opens, commits, or rolls back transactions and returns ErrMissingTx when no transaction is
// active in ctx.
type TxEventRepository[E any] struct {
	core *eventRepoCore[E]
}

// NewTxEventRepository validates cfg and returns a context-transaction TxEventRepository[E].
func NewTxEventRepository[E any](cfg TxEventRepositoryConfig[E]) (*TxEventRepository[E], error) { //nolint:gocritic // hugeParam: value config simplifies caller literals
	core, err := newEventRepoCore(cfg)
	if err != nil {
		return nil, err
	}
	return &TxEventRepository[E]{core: core}, nil
}

// Load retrieves the active transaction from ctx via RequireTx and loads the stream for id.
func (r *TxEventRepository[E]) Load(
	ctx context.Context,
	id string,
) (events []E, version int64, err error) {
	tx, err := RequireTx(ctx)
	if err != nil {
		return nil, 0, err
	}
	return r.core.loadTx(ctx, tx, id)
}

// Append retrieves the active transaction from ctx via RequireTx and appends events with
// inline projections inside that transaction.
func (r *TxEventRepository[E]) Append(ctx context.Context, id string, expectedVersion int64, events []E) error {
	tx, err := RequireTx(ctx)
	if err != nil {
		return err
	}
	return r.core.appendTx(ctx, tx, id, expectedVersion, events)
}

// LoadTx reads and decodes the persisted event stream for id using caller-managed tx.
func (r *TxEventRepository[E]) LoadTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (events []E, version int64, err error) {
	return r.core.loadTx(ctx, tx, id)
}

// AppendTx encodes domain events and appends them with inline projections using caller-managed tx.
func (r *TxEventRepository[E]) AppendTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	expectedVersion int64,
	events []E,
) error {
	return r.core.appendTx(ctx, tx, id, expectedVersion, events)
}

func (c *eventRepoCore[E]) loadTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (events []E, version int64, err error) {
	if ctx == nil {
		return nil, 0, persistence.ErrNilContext
	}
	if isNilValue(tx) {
		return nil, 0, ErrNilTx
	}
	if strings.TrimSpace(id) == "" {
		return nil, 0, persistence.ErrEmptyTargetID
	}

	stream, err := c.reader.ReadStream(ctx, tx, c.streamType, id, nil, nil)
	if err != nil {
		return nil, 0, err
	}
	if stream.IsEmpty() {
		return nil, 0, nil
	}
	decoded := make([]E, len(stream.Events))
	for i := range stream.Events {
		ev, decErr := c.codec.Decode(ctx, stream.Events[i])
		if decErr != nil {
			return nil, 0, decErr
		}
		decoded[i] = ev
	}
	return decoded, stream.Version(), nil
}

func (c *eventRepoCore[E]) appendTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	expectedVersion int64,
	events []E,
) error {
	if ctx == nil {
		return persistence.ErrNilContext
	}
	if isNilValue(tx) {
		return ErrNilTx
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

	storeEvents, err := c.encodeEvents(ctx, id, events)
	if err != nil {
		return err
	}
	return c.appendEncodedInTx(ctx, tx, expectedVersion, storeEvents)
}

func (c *eventRepoCore[E]) encodeEvents(ctx context.Context, id string, events []E) ([]store.Event, error) {
	obs, _ := cqrs.ObservabilityFromContext(ctx)
	causationID := firstNonEmpty(obs.CommandID, obs.CausationID)

	encoded := make([]store.Event, len(events))
	for i := range events {
		se, err := c.codec.Encode(ctx, c.streamType, id, events[i])
		if err != nil {
			return nil, err
		}
		if err := c.normalizeEvent(&se, id, obs.TraceID, causationID); err != nil {
			return nil, err
		}
		encoded[i] = se
	}
	return encoded, nil
}

func (c *eventRepoCore[E]) normalizeEvent(
	se *store.Event,
	id string,
	traceID string,
	causationID string,
) error {
	if se.StreamType == "" {
		se.StreamType = c.streamType
	} else if se.StreamType != c.streamType {
		return ErrStreamMismatch
	}
	if se.StreamID == "" {
		se.StreamID = id
	} else if se.StreamID != id {
		return ErrStreamMismatch
	}
	if strings.TrimSpace(se.EventType) == "" {
		return ErrEmptyEventType
	}
	if se.EventID == uuid.Nil {
		se.EventID = c.idGen()
	}
	if se.EventVersion <= 0 {
		se.EventVersion = 1
	}
	if se.CreatedAt.IsZero() {
		se.CreatedAt = c.clock()
	}
	if !se.TraceID.Valid && traceID != "" {
		se.TraceID = store.NullString{String: traceID, Valid: true}
	}
	if !se.CausationID.Valid && causationID != "" {
		se.CausationID = store.NullString{String: causationID, Valid: true}
	}
	return nil
}

func (c *eventRepoCore[E]) appendEncodedInTx(
	ctx context.Context,
	tx pgx.Tx,
	expectedVersion int64,
	storeEvents []store.Event,
) error {
	res, err := c.eventStore.Append(ctx, tx, store.Exact(expectedVersion), storeEvents)
	if err != nil {
		if errors.Is(err, store.ErrOptimisticConcurrency) {
			return wrapConcurrencyConflict(err)
		}
		return err
	}

	return c.runProjections(ctx, tx, res.Events)
}

func (c *eventRepoCore[E]) runProjections(ctx context.Context, tx pgx.Tx, persisted []store.PersistedEvent) error {
	if len(persisted) == 0 {
		return nil
	}
	for _, bp := range c.batchProjections {
		if err := bp.Handle(ctx, tx, persisted); err != nil {
			return err
		}
	}
	for _, p := range c.projections {
		for i := range persisted {
			if err := p.Handle(ctx, tx, persisted[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
