package persistence

import (
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/snapshot"
	"eventsalsa.dev/store"
)

const defaultSnapshotInterval = 3

type orderSnapshotConfig struct {
	policy        snapshot.Policy
	streamType    string
	schemaVersion int
	compress      bool
}

// OrderSnapshotOption configures snapshot settings for NewOrderSnapshotAdapter.
type OrderSnapshotOption func(*orderSnapshotConfig)

// WithStreamType overrides the event store stream type partition (default "Order").
func WithStreamType(streamType string) OrderSnapshotOption {
	return func(c *orderSnapshotConfig) {
		c.streamType = streamType
	}
}

// WithSchemaVersion overrides the snapshot payload schema version (default 1).
func WithSchemaVersion(version int) OrderSnapshotOption {
	return func(c *orderSnapshotConfig) {
		c.schemaVersion = version
	}
}

// WithSnapshotPolicy overrides when snapshots are written (default snapshot.EveryNEvents(3)).
func WithSnapshotPolicy(policy snapshot.Policy) OrderSnapshotOption {
	return func(c *orderSnapshotConfig) {
		c.policy = policy
	}
}

// WithSnapshotCompression toggles gzip compression for serialized snapshot payloads (default true).
func WithSnapshotCompression(enabled bool) OrderSnapshotOption {
	return func(c *orderSnapshotConfig) {
		c.compress = enabled
	}
}

// OrderDecider returns the pure persistence.Decider for Order streams.
func OrderDecider() persistence.Decider[domain.Order, any] {
	return persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
}

// NewOrderSnapshotAdapter builds the narrow-transaction snapshotted Order adapter backed by
// eventmap-gen codecs, inline OrderOverviewProjection, and eventsalsa/snapshot.
func NewOrderSnapshotAdapter(
	db postgres.TxBeginner,
	eventStore store.EventStore,
	snapStore snapshot.Store,
	opts ...OrderSnapshotOption,
) (persistence.Adapter[domain.Order, any], error) {
	cfg := orderSnapshotConfig{
		streamType:    "Order",
		schemaVersion: 1,
		policy:        snapshot.EveryNEvents(defaultSnapshotInterval),
		compress:      true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	codec, err := postgres.NewEventMapCodec(MarshalEvent, FromESEventAs[any])
	if err != nil {
		return persistence.Adapter[domain.Order, any]{}, err
	}

	var transformers []snapshot.PayloadTransformer
	if cfg.compress {
		transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}
	}

	return postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[domain.Order, any]{
		Decider: OrderDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          db,
			EventStore:  eventStore,
			StreamType:  cfg.streamType,
			Codec:       codec,
			Projections: []postgres.Projection{&OrderOverviewProjection{}},
		},
		Snapshot: postgres.SnapshotConfig[domain.Order]{
			Store:         snapStore,
			Codec:         snapshot.JSON[domain.Order](),
			SchemaVersion: cfg.schemaVersion,
			Policy:        cfg.policy,
			Version:       func(o domain.Order) int64 { return o.Version },
			Transformers:  transformers,
		},
	})
}
