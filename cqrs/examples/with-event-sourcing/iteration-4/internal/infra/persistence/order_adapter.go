package persistence

import (
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/config"
	"eventsalsa.dev/cqrs/examples/with-event-sourcing/iteration-4/internal/domain"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/snapshot"
	"eventsalsa.dev/store"
)

// OrderAdapterParams supplies infrastructure dependencies for constructing the Order adapter.
type OrderAdapterParams struct {
	DB         postgres.TxBeginner
	EventStore store.EventStore
	SnapStore  snapshot.Store
	Snapshot   config.SnapshotConfig
}

// OrderDecider returns the pure persistence.Decider for Order streams.
func OrderDecider() persistence.Decider[domain.Order, any] {
	return persistence.Decider[domain.Order, any]{
		Initial: domain.InitialOrder,
		Evolve:  domain.EvolveOrder,
	}
}

// NewOrderAdapter builds the narrow-transaction snapshotted Order adapter backed by
// eventmap-gen codecs, inline OrderOverviewProjection, and eventsalsa/snapshot.
func NewOrderAdapter(params *OrderAdapterParams) (persistence.Adapter[domain.Order, any], error) {
	if params == nil {
		return persistence.Adapter[domain.Order, any]{}, postgres.ErrNilDB
	}
	codec, err := postgres.NewEventMapCodec(MarshalEvent, FromESEventAs[any])
	if err != nil {
		return persistence.Adapter[domain.Order, any]{}, err
	}

	var transformers []snapshot.PayloadTransformer
	if params.Snapshot.Compress {
		transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}
	}

	return postgres.NewSnapshotAdapter(postgres.SnapshotAdapterConfig[domain.Order, any]{
		Decider: OrderDecider(),
		EventRepository: postgres.EventRepositoryConfig[any]{
			DB:          params.DB,
			EventStore:  params.EventStore,
			StreamType:  params.Snapshot.StreamType,
			Codec:       codec,
			Projections: []postgres.Projection{&OrderOverviewProjection{}},
		},
		Snapshot: postgres.SnapshotConfig[domain.Order]{
			Store:         params.SnapStore,
			Codec:         snapshot.JSON[domain.Order](),
			SchemaVersion: params.Snapshot.SchemaVersion,
			Policy:        snapshot.EveryNEvents(params.Snapshot.EveryNEvents),
			Version:       func(o domain.Order) int64 { return o.Version },
			Transformers:  transformers,
		},
	})
}
