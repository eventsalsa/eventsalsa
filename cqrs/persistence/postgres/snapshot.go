package postgres

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/snapshot"
	"eventsalsa.dev/store"
)

// SnapshotConfig configures snapshot loading, upcasting, transformation, and write-side persistence
// backed by eventsalsa.dev/snapshot.
type SnapshotConfig[S any] struct {
	// Store persists and loads raw snapshot payloads (for example *snapshot/postgres.Store). Required.
	Store snapshot.Store

	// Codec serializes state S to and from raw snapshot bytes (for example snapshot.JSON[S]()). Required.
	Codec snapshot.Codec[S]

	// Logger optionally logs snapshot fallback warnings and best-effort save failures.
	Logger store.Logger

	// Upcasters migrates older schema payloads sequentially up to SchemaVersion.
	Upcasters map[int]snapshot.Upcaster

	// Policy controls when an append triggers writing a new snapshot. Required.
	Policy snapshot.Policy

	// Version optionally checks state S's embedded version against the target snapshot version on Save.
	Version func(state S) int64

	// OnSnapshotRejected is notified when a loaded snapshot is corrupt, incompatible, or missing from
	// the event log and full stream replay is used instead (when FailOnCorruptSnapshot is false).
	OnSnapshotRejected func(ctx context.Context, streamID string, reason string, err error)

	// OnSnapshotSaveError is notified when a snapshot Save fails in best-effort mode
	// (when FailOnSnapshotSaveError is false).
	OnSnapshotSaveError func(ctx context.Context, streamID string, version int64, err error)

	// Transformers specifies optional outbound/inbound payload transformations (e.g., Gzip, encryption).
	Transformers []snapshot.PayloadTransformer

	// SchemaVersion identifies the active snapshot payload schema version (must be > 0).
	SchemaVersion int

	// FailOnCorruptSnapshot causes snapshot load, transform, or upcast failures to abort loading
	// instead of falling back to full stream replay.
	FailOnCorruptSnapshot bool

	// FailOnSnapshotSaveError causes snapshot save failures to abort and roll back the write
	// transaction instead of isolating the failure in a savepoint so domain events commit.
	FailOnSnapshotSaveError bool
}

// SnapshotAdapterConfig configures a narrow-transaction snapshotted persistence.Adapter[S, E].
//
// Reads run inside InReadTx and release the connection before domain decide logic runs; appends,
// inline projections, and conditional snapshot writes run inside InTx. Invoking the adapter inside
// an active context transaction fails immediately with ErrTxAlreadyActive.
type SnapshotAdapterConfig[S any, E any] struct {
	Decider         persistence.Decider[S, E]
	EventRepository EventRepositoryConfig[E]
	Snapshot        SnapshotConfig[S]
}

// TxSnapshotAdapterConfig configures a wide-transaction snapshotted persistence.Adapter[S, E].
//
// Reads, appends, inline projections, and conditional snapshot writes all run inside the active
// transaction retrieved via RequireTx(ctx), returning ErrMissingTx when none is active.
type TxSnapshotAdapterConfig[S any, E any] struct {
	Decider           persistence.Decider[S, E]
	TxEventRepository TxEventRepositoryConfig[E]
	Snapshot          SnapshotConfig[S]
}

type validatedSnapshotConfig[S any] struct {
	store                   snapshot.Store
	codec                   snapshot.Codec[S]
	logger                  store.Logger
	upcasters               map[int]snapshot.Upcaster
	policy                  snapshot.Policy
	version                 func(state S) int64
	onRejected              func(ctx context.Context, streamID string, reason string, err error)
	onSaveError             func(ctx context.Context, streamID string, version int64, err error)
	transformers            []snapshot.PayloadTransformer
	schemaVersion           int
	failOnCorruptSnapshot   bool
	failOnSnapshotSaveError bool
}

type snapshotCore[S any, E any] struct {
	core     *eventRepoCore[E]
	saveRepo *snapshot.Repository[S]
	decider  persistence.Decider[S, E]
	snap     validatedSnapshotConfig[S]
}

// NewSnapshotAdapter validates cfg and returns a narrow-transaction snapshotted
// persistence.Adapter[S, E] backed by eventsalsa.dev/store and eventsalsa.dev/snapshot.
func NewSnapshotAdapter[S any, E any](
	cfg SnapshotAdapterConfig[S, E], //nolint:gocritic // hugeParam: value config simplifies caller literals
) (persistence.Adapter[S, E], error) {
	if isNilValue(cfg.EventRepository.DB) {
		return persistence.Adapter[S, E]{}, ErrNilDB
	}
	sc, err := newSnapshotCore(
		TxEventRepositoryConfig[E]{
			EventStore:       cfg.EventRepository.EventStore,
			Reader:           cfg.EventRepository.Reader,
			Codec:            cfg.EventRepository.Codec,
			Clock:            cfg.EventRepository.Clock,
			IDGen:            cfg.EventRepository.IDGen,
			StreamType:       cfg.EventRepository.StreamType,
			Projections:      cfg.EventRepository.Projections,
			BatchProjections: cfg.EventRepository.BatchProjections,
		},
		cfg.Decider,
		cfg.Snapshot,
	)
	if err != nil {
		return persistence.Adapter[S, E]{}, err
	}
	db := cfg.EventRepository.DB
	return persistence.NewAdapter(func(ctx context.Context, id string, decide func(state S) ([]E, error)) error {
		return sc.execute(ctx, db, id, decide)
	}), nil
}

// NewTxSnapshotAdapter validates cfg and returns a wide-transaction snapshotted
// persistence.Adapter[S, E] participating in an active context transaction via RequireTx.
func NewTxSnapshotAdapter[S any, E any](
	cfg TxSnapshotAdapterConfig[S, E], //nolint:gocritic // hugeParam: value config simplifies caller literals
) (persistence.Adapter[S, E], error) {
	sc, err := newSnapshotCore(cfg.TxEventRepository, cfg.Decider, cfg.Snapshot)
	if err != nil {
		return persistence.Adapter[S, E]{}, err
	}
	return persistence.NewAdapter(func(ctx context.Context, id string, decide func(state S) ([]E, error)) error {
		return sc.execute(ctx, nil, id, decide)
	}), nil
}

//nolint:gocritic // hugeParam: internal builder
func newSnapshotCore[S any, E any](
	evCfg TxEventRepositoryConfig[E],
	decider persistence.Decider[S, E],
	snapCfg SnapshotConfig[S],
) (*snapshotCore[S, E], error) {
	if decider.Initial == nil {
		return nil, persistence.ErrNilInitial
	}
	if decider.Evolve == nil {
		return nil, persistence.ErrNilEvolve
	}
	core, err := newEventRepoCore(evCfg)
	if err != nil {
		return nil, err
	}
	if isNilValue(snapCfg.Store) {
		return nil, ErrNilSnapshotStore
	}
	if isNilValue(snapCfg.Codec) {
		return nil, ErrNilSnapshotCodec
	}
	if snapCfg.Policy == nil {
		return nil, ErrNilSnapshotPolicy
	}

	transformers := slices.Clone(snapCfg.Transformers)
	for _, tr := range transformers {
		if isNilValue(tr) {
			return nil, ErrNilPayloadTransformer
		}
	}

	validatedSnap := validatedSnapshotConfig[S]{
		transformers:            transformers,
		store:                   snapCfg.Store,
		codec:                   snapCfg.Codec,
		logger:                  snapCfg.Logger,
		upcasters:               maps.Clone(snapCfg.Upcasters),
		policy:                  snapCfg.Policy,
		version:                 snapCfg.Version,
		onRejected:              snapCfg.OnSnapshotRejected,
		onSaveError:             snapCfg.OnSnapshotSaveError,
		schemaVersion:           snapCfg.SchemaVersion,
		failOnCorruptSnapshot:   snapCfg.FailOnCorruptSnapshot,
		failOnSnapshotSaveError: snapCfg.FailOnSnapshotSaveError,
	}

	sc := &snapshotCore[S, E]{
		core:    core,
		decider: decider,
		snap:    validatedSnap,
	}

	// Validate SchemaVersion, Upcasters, and repository wiring eagerly on construction.
	saveRepo, err := sc.newRepo(func(state S, _ store.PersistedEvent) (S, error) {
		return state, nil
	})
	if err != nil {
		return nil, err
	}
	sc.saveRepo = saveRepo
	return sc, nil
}

func (s *snapshotCore[S, E]) newRepo(
	apply func(state S, event store.PersistedEvent) (S, error),
) (*snapshot.Repository[S], error) {
	return snapshot.NewRepository(s.core.reader, s.snap.store, snapshot.RepositoryConfig[S]{
		StreamType:            s.core.streamType,
		SchemaVersion:         s.snap.schemaVersion,
		Initializer:           s.decider.Initial,
		Apply:                 apply,
		Codec:                 s.snap.codec,
		Version:               s.snap.version,
		Upcasters:             s.snap.upcasters,
		Transformers:          s.snap.transformers,
		Logger:                s.snap.logger,
		OnSnapshotRejected:    s.snap.onRejected,
		FailOnCorruptSnapshot: s.snap.failOnCorruptSnapshot,
	})
}

func (s *snapshotCore[S, E]) loadInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (snapshot.Result[S], error) {
	apply := func(state S, pe store.PersistedEvent) (S, error) {
		ev, err := s.core.codec.Decode(ctx, pe)
		if err != nil {
			var zero S
			return zero, err
		}
		return s.decider.Evolve(state, ev), nil
	}
	repo, err := s.newRepo(apply)
	if err != nil {
		return snapshot.Result[S]{}, err
	}
	return repo.Load(ctx, tx, id)
}

func (s *snapshotCore[S, E]) execute(
	ctx context.Context,
	db TxBeginner,
	id string,
	decide func(state S) ([]E, error),
) error {
	if ctx == nil {
		return persistence.ErrNilContext
	}
	if strings.TrimSpace(id) == "" {
		return persistence.ErrEmptyTargetID
	}
	if decide == nil {
		return persistence.ErrNilDecide
	}

	var loadRes snapshot.Result[S]
	if err := s.runInTxMode(ctx, db, true, func(txCtx context.Context, tx pgx.Tx) error {
		var loadErr error
		loadRes, loadErr = s.loadInTx(txCtx, tx, id)
		return loadErr
	}); err != nil {
		return err
	}
	if loadRes.StreamVersion < 0 {
		return persistence.ErrInvalidVersion
	}

	events, err := decide(loadRes.State)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	storeEvents, err := s.core.encodeEvents(ctx, id, events)
	if err != nil {
		return err
	}
	return s.runInTxMode(ctx, db, false, func(txCtx context.Context, tx pgx.Tx) error {
		return s.commitInTx(txCtx, tx, id, loadRes, events, storeEvents)
	})
}

func (s *snapshotCore[S, E]) runInTxMode(
	ctx context.Context,
	db TxBeginner,
	readOnly bool,
	fn func(ctx context.Context, tx pgx.Tx) error,
) error {
	if db != nil {
		if readOnly {
			return InReadTx(ctx, db, fn)
		}
		return InTx(ctx, db, fn)
	}
	tx, err := RequireTx(ctx)
	if err != nil {
		return err
	}
	return fn(ctx, tx)
}

func (s *snapshotCore[S, E]) commitInTx(
	txCtx context.Context,
	tx pgx.Tx,
	id string,
	loadRes snapshot.Result[S],
	events []E,
	storeEvents []store.Event,
) error {
	if err := s.core.appendEncodedInTx(txCtx, tx, loadRes.StreamVersion, storeEvents); err != nil {
		return err
	}
	if !loadRes.ShouldSnapshot(s.snap.policy, int64(len(events))) {
		return nil
	}

	nextState := loadRes.State
	for i := range events {
		nextState = s.decider.Evolve(nextState, events[i])
	}
	nextVersion := loadRes.StreamVersion + int64(len(events))
	return s.saveSnapshot(txCtx, tx, id, nextVersion, nextState)
}

func (s *snapshotCore[S, E]) saveSnapshot(
	txCtx context.Context,
	tx pgx.Tx,
	id string,
	version int64,
	state S,
) error {
	if s.snap.failOnSnapshotSaveError {
		return s.saveRepo.Save(txCtx, tx, id, version, state)
	}

	subTx, err := tx.Begin(txCtx)
	if err != nil {
		return err
	}
	if saveErr := s.saveRepo.Save(txCtx, subTx, id, version, state); saveErr != nil {
		if txCtx.Err() != nil || errors.Is(saveErr, context.Canceled) || errors.Is(saveErr, context.DeadlineExceeded) {
			return saveErr
		}
		rbCtx, cancel := context.WithTimeout(context.WithoutCancel(txCtx), defaultRollbackTimeout)
		rbErr := subTx.Rollback(rbCtx)
		cancel()
		if rbErr != nil {
			return errors.Join(saveErr, rbErr)
		}
		if s.snap.logger != nil {
			s.snap.logger.Error(txCtx, "snapshot save failed; continuing without snapshot",
				"stream_type", s.core.streamType,
				"stream_id", id,
				"stream_version", version,
				"error", saveErr,
			)
		}
		if s.snap.onSaveError != nil {
			s.snap.onSaveError(txCtx, id, version, saveErr)
		}
		return nil
	}
	return subTx.Commit(txCtx)
}
