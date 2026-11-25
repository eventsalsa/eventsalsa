// Package main demonstrates CQRS combined with Event Sourcing over eventsalsa/store.
//
// Write-side commands execute inside a PostgreSQL transaction middleware, fold the
// Order stream via pure initial/evolve/decide functions, append events to
// eventsalsa.dev/store with optimistic concurrency + causal metadata, and update an
// inline read-model table. Read-side queries query that table directly via query.Execute.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/query"
	"eventsalsa.dev/store"
	"eventsalsa.dev/store/migrations"
	storepostgres "eventsalsa.dev/store/postgres"
)

const (
	streamTypeOrder = "Order"
	eventPlaced     = "OrderPlaced"
	eventConfirmed  = "OrderConfirmed"
)

type txCtxKey struct{}

func withPgxTx(pool *pgxpool.Pool) command.Middleware {
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) (retErr error) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			committed := false
			defer func() {
				if !committed {
					if rbErr := tx.Rollback(ctx); rbErr != nil && retErr == nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
						retErr = rbErr
					}
				}
			}()

			if nextErr := next(context.WithValue(ctx, txCtxKey{}, tx), cmd); nextErr != nil {
				return nextErr
			}
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return commitErr
			}
			committed = true
			return nil
		}
	}
}

func txFromContext(ctx context.Context) (pgx.Tx, error) {
	tx, ok := ctx.Value(txCtxKey{}).(pgx.Tx)
	if !ok {
		return nil, errors.New("missing pgx.Tx in context")
	}
	return tx, nil
}

// GetOrderOverview fetches a read-model row from order_overview_v1.
type GetOrderOverview struct {
	OrderID string
}

// OrderOverview is a projected read DTO queried independently of aggregate streams.
type OrderOverview struct {
	OrderID    string
	CustomerID string
	Status     string
	TotalCents int64
	Version    int64
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	cfg := migrations.DefaultConfig()
	ddl, err := migrations.SQL(&cfg)
	if err != nil {
		return fmt.Errorf("build store DDL: %w", err)
	}
	if _, execErr := pool.Exec(ctx, ddl); execErr != nil {
		return fmt.Errorf("apply store DDL: %w", execErr)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS order_overview_v1 (
			order_id    TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			status      TEXT NOT NULL,
			total_cents BIGINT NOT NULL,
			version     BIGINT NOT NULL
		)
	`)
	return err
}

func loadOrderStream(
	ctx context.Context,
	tx pgx.Tx,
	es *storepostgres.Store,
	orderID string,
) (OrderState, store.ExpectedVersion, error) {
	stream, err := es.ReadStream(ctx, tx, streamTypeOrder, orderID, nil, nil)
	if err != nil {
		return OrderState{}, store.ExpectedVersion{}, err
	}
	state := initialOrder(orderID)
	for i := range stream.Events {
		ev := &stream.Events[i]
		domainEv, decErr := decodeDomainEvent(ev.EventType, ev.Payload)
		if decErr != nil {
			return OrderState{}, store.ExpectedVersion{}, decErr
		}
		state = evolveOrder(state, domainEv)
	}
	expected := store.NoStream()
	if stream.Version() > 0 {
		expected = store.Exact(stream.Version())
	}
	return state, expected, nil
}

func decodeDomainEvent(eventType string, payload []byte) (any, error) {
	switch eventType {
	case eventPlaced:
		var ev OrderPlaced
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	case eventConfirmed:
		var ev OrderConfirmed
		if err := json.Unmarshal(payload, &ev); err != nil {
			return nil, err
		}
		return ev, nil
	default:
		return nil, fmt.Errorf("unknown event type %q", eventType)
	}
}

func encodeStoreEvents(ctx context.Context, orderID string, domainEvents []any) ([]store.Event, error) {
	obs, _ := cqrs.ObservabilityFromContext(ctx)
	traceID := store.NullString{String: obs.TraceID, Valid: obs.TraceID != ""}
	causationID := store.NullString{String: obs.CommandID, Valid: obs.CommandID != ""}

	out := make([]store.Event, 0, len(domainEvents))
	now := time.Now().UTC()
	for _, ev := range domainEvents {
		var eventType string
		switch ev.(type) {
		case OrderPlaced:
			eventType = eventPlaced
		case OrderConfirmed:
			eventType = eventConfirmed
		default:
			return nil, fmt.Errorf("unsupported domain event %T", ev)
		}
		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Event{
			StreamType:   streamTypeOrder,
			StreamID:     orderID,
			EventID:      uuid.New(),
			EventType:    eventType,
			EventVersion: 1,
			Payload:      payload,
			Metadata:     []byte(`{}`),
			TraceID:      traceID,
			CausationID:  causationID,
			CreatedAt:    now,
		})
	}
	return out, nil
}

// projectInline updates order_overview_v1 inside the write transaction.
// When projecting asynchronously instead, implement projector.Projection from eventsalsa/projector.
func projectInline(ctx context.Context, tx pgx.Tx, persisted []store.PersistedEvent) error {
	for i := range persisted {
		ev := &persisted[i]
		domainEv, err := decodeDomainEvent(ev.EventType, ev.Payload)
		if err != nil {
			return err
		}
		switch d := domainEv.(type) {
		case OrderPlaced:
			_, execErr := tx.Exec(ctx, `
				INSERT INTO order_overview_v1 (order_id, customer_id, status, total_cents, version)
				VALUES ($1, $2, 'PLACED', $3, $4)
				ON CONFLICT (order_id) DO UPDATE
				SET customer_id = EXCLUDED.customer_id,
					status      = EXCLUDED.status,
					total_cents = EXCLUDED.total_cents,
					version     = EXCLUDED.version
				WHERE order_overview_v1.version < EXCLUDED.version
			`, ev.StreamID, d.CustomerID, d.TotalCents, ev.StreamVersion)
			if execErr != nil {
				return execErr
			}
		case OrderConfirmed:
			_, execErr := tx.Exec(ctx, `
				UPDATE order_overview_v1
				SET status = 'CONFIRMED', version = $2
				WHERE order_id = $1 AND version < $2
			`, ev.StreamID, ev.StreamVersion)
			if execErr != nil {
				return execErr
			}
		}
	}
	return nil
}

func appendAndProject[C any](
	ctx context.Context,
	es *storepostgres.Store,
	orderID string,
	cmd C,
	decide func(OrderState, C) ([]any, error),
) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	state, expected, err := loadOrderStream(ctx, tx, es, orderID)
	if err != nil {
		return err
	}
	domainEvents, err := decide(state, cmd)
	if err != nil {
		return err
	}
	if len(domainEvents) == 0 {
		return nil
	}
	toAppend, err := encodeStoreEvents(ctx, orderID, domainEvents)
	if err != nil {
		return err
	}
	res, err := es.Append(ctx, tx, expected, toAppend)
	if err != nil {
		return err
	}
	return projectInline(ctx, tx, res.Events)
}

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// #nosec G101 -- local development example default DSN
		dsn = "postgres://postgres:postgres@localhost:5432/eventsalsa?sslmode=disable" //nolint:gosec // G101: default local DSN
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if schemaErr := ensureSchema(ctx, pool); schemaErr != nil {
		return schemaErr
	}

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	cmdBus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(),
			command.RetryMiddleware(command.RetryConfig{
				MaxAttempts: 3,
				RetryIf: func(err error) bool {
					return errors.Is(err, store.ErrOptimisticConcurrency)
				},
			}),
			withPgxTx(pool),
		),
		command.WithErrorClassifier(func(err error) error {
			if errors.Is(err, errAlreadyPlaced) || errors.Is(err, errNotPlaced) || errors.Is(err, errInvalidAmount) {
				return command.Reject(err)
			}
			return err
		}),
	)
	if err != nil {
		return err
	}

	if regErr := command.Register(cmdBus, command.Func[PlaceOrder](func(ctx context.Context, cmd PlaceOrder) error {
		return appendAndProject(ctx, es, cmd.OrderID, cmd, decidePlaceOrder)
	})); regErr != nil {
		return regErr
	}
	if regErr := command.Register(cmdBus, command.Func[ConfirmOrder](func(ctx context.Context, cmd ConfirmOrder) error {
		return appendAndProject(ctx, es, cmd.OrderID, cmd, decideConfirmOrder)
	})); regErr != nil {
		return regErr
	}

	qBus, err := query.NewBus(query.WithMiddleware(query.ObservabilityMiddleware()))
	if err != nil {
		return err
	}
	if regErr := query.Register(qBus, query.Func[GetOrderOverview, OrderOverview](
		func(ctx context.Context, q GetOrderOverview) (OrderOverview, error) {
			var view OrderOverview
			row := pool.QueryRow(ctx, `
				SELECT order_id, customer_id, status, total_cents, version
				FROM order_overview_v1
				WHERE order_id = $1
			`, q.OrderID)
			scanErr := row.Scan(&view.OrderID, &view.CustomerID, &view.Status, &view.TotalCents, &view.Version)
			return view, scanErr
		},
	)); regErr != nil {
		return regErr
	}

	orderID := "ord-" + uuid.NewString()[:8]
	reqCtx := cqrs.WithTraceID(ctx, "trace-"+uuid.NewString()[:8])

	if dispatchErr := cmdBus.Dispatch(reqCtx, PlaceOrder{
		OrderID:    orderID,
		CustomerID: "cust-42",
		TotalCents: 12900,
	}); dispatchErr != nil {
		return dispatchErr
	}
	if dispatchErr := cmdBus.Dispatch(reqCtx, ConfirmOrder{OrderID: orderID}); dispatchErr != nil {
		return dispatchErr
	}

	view, err := query.Execute[OrderOverview](reqCtx, qBus, GetOrderOverview{OrderID: orderID})
	if err != nil {
		return err
	}
	fmt.Printf("Projected overview: %+v\n", view)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
