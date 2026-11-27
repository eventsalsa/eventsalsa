//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"eventsalsa.dev/cqrs/persistence"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
	"eventsalsa.dev/store/migrations"
)

var testDBConnStr string

func TestMain(m *testing.M) {
	code, err := runMain(m)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func runMain(m *testing.M) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("eventsalsa_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return 1, fmt.Errorf("start postgres container: %w", err)
	}
	defer func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer termCancel()
		if termErr := pgContainer.Terminate(termCtx); termErr != nil {
			log.Printf("terminate postgres container: %v", termErr)
		}
	}()

	testDBConnStr, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 1, fmt.Errorf("connection string: %w", err)
	}

	return m.Run(), nil
}

func setupPool(t *testing.T, customize ...func(*pgxpool.Config)) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(testDBConnStr)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}

	switch os.Getenv("PGX_TEST_QUERY_EXEC_MODE") {
	case "cache_statement":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	case "cache_describe":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	case "describe_exec":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	case "exec":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	case "simple_protocol":
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	for _, fn := range customize {
		if fn != nil {
			fn(cfg)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	ensureSchema(ctx, t, pool)
	return pool
}

func ensureSchema(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	migCfg := migrations.DefaultConfig()
	ddl, err := migrations.SQL(&migCfg)
	if err != nil {
		t.Fatalf("generate store DDL: %v", err)
	}
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("install store DDL: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS order_overview_v1 (
			order_id TEXT PRIMARY KEY,
			balance  INT NOT NULL,
			version  BIGINT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS state_accounts (
			account_id TEXT PRIMARY KEY,
			balance    INT NOT NULL,
			version    BIGINT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS audit_log (
			id      BIGSERIAL PRIMARY KEY,
			message TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("install app tables: %v", err)
	}
}

type accountCredited struct {
	Amount int `json:"amount"`
}

type accountState struct {
	ID      string
	Balance int
}

func accountDecider() persistence.Decider[accountState, any] {
	return persistence.Decider[accountState, any]{
		Initial: func(id string) accountState {
			return accountState{ID: id}
		},
		Evolve: func(state accountState, ev any) accountState {
			if c, ok := ev.(accountCredited); ok {
				state.Balance += c.Amount
			}
			return state
		},
	}
}

func eventMapAccountCodec() (postgres.EventCodec[any], error) {
	return postgres.NewEventMapCodec(
		func(ev any) (string, int, []byte, error) {
			c, ok := ev.(accountCredited)
			if !ok {
				return "", 0, nil, fmt.Errorf("%w: %T", store.ErrUnknownEvent, ev)
			}
			raw, err := json.Marshal(c)
			return "AccountCredited", 1, raw, err
		},
		func(pe store.PersistedEvent) (any, error) {
			if pe.EventType != "AccountCredited" {
				return nil, fmt.Errorf("%w: %s", store.ErrUnknownEvent, pe.EventType)
			}
			var c accountCredited
			if err := json.Unmarshal(pe.Payload, &c); err != nil {
				return nil, err
			}
			return c, nil
		},
	)
}

func accountBalanceProjection(codec postgres.EventCodec[any]) postgres.Projection {
	return postgres.ProjectionFunc(func(ctx context.Context, tx pgx.Tx, ev store.PersistedEvent) error {
		decoded, err := codec.Decode(ctx, ev)
		if err != nil {
			return err
		}
		credited, ok := decoded.(accountCredited)
		if !ok {
			return fmt.Errorf("unexpected decoded event %T", decoded)
		}
		_, execErr := tx.Exec(ctx, `
			INSERT INTO order_overview_v1 (order_id, balance, version)
			VALUES ($1, $2, $3)
			ON CONFLICT (order_id) DO UPDATE
			SET balance = order_overview_v1.balance + EXCLUDED.balance,
				version = EXCLUDED.version
			WHERE order_overview_v1.version < EXCLUDED.version
		`, ev.StreamID, credited.Amount, ev.StreamVersion)
		return execErr
	})
}
