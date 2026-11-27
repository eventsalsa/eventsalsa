//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/cqrs/query"
	storepostgres "eventsalsa.dev/store/postgres"
)

func TestSimpleProtocolMode_EndToEnd(t *testing.T) {
	pool := setupPool(t, func(c *pgxpool.Config) {
		c.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	})
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	codec, err := eventMapAccountCodec()
	if err != nil {
		t.Fatalf("codec: %v", err)
	}

	repo, err := postgres.NewEventRepository(postgres.EventRepositoryConfig[any]{
		DB:         pool,
		EventStore: es,
		StreamType: "SimpleProto",
		Codec:      codec,
		Projections: []postgres.Projection{
			accountBalanceProjection(codec),
		},
	})
	if err != nil {
		t.Fatalf("NewEventRepository: %v", err)
	}

	bus, err := command.NewBus(
		command.WithMiddleware(command.ObservabilityMiddleware()),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, persistence.MustNewCommandHandler(
		persistence.FromEventRepository(repo, accountDecider()),
		func(c creditCmd) string { return c.id },
		func(_ accountState, c creditCmd) ([]any, error) {
			return []any{accountCredited{Amount: c.amount}}, nil
		},
	))

	id := "simple-" + uuid.NewString()
	// First event with trace ID, plus direct Append without trace/causation (NULL columns in SQL).
	if dispatchErr := bus.Dispatch(cqrs.WithTraceID(ctx, "trace-simple"), creditCmd{id: id, amount: 30}); dispatchErr != nil {
		t.Fatalf("Dispatch with trace: %v", dispatchErr)
	}
	if appErr := repo.Append(ctx, id, 1, []any{accountCredited{Amount: 20}}); appErr != nil {
		t.Fatalf("Append with NULL trace/causation: %v", appErr)
	}

	qBus, err := query.NewBus(query.WithMiddleware(postgres.MustQueryTxMiddleware(pool)))
	if err != nil {
		t.Fatalf("query.NewBus: %v", err)
	}
	query.MustRegister(qBus, query.Func[string, int](func(qCtx context.Context, targetID string) (int, error) {
		tx, _ := postgres.TxFromContext(qCtx)
		var bal int
		qErr := tx.QueryRow(qCtx, `SELECT balance FROM order_overview_v1 WHERE order_id = $1`, targetID).Scan(&bal)
		return bal, qErr
	}))

	bal, err := query.Execute[int](ctx, qBus, id)
	if err != nil || bal != 50 {
		t.Fatalf("query in simple protocol = (%d, %v), want (50, nil)", bal, err)
	}
}
