package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/store"
	storemigrations "eventsalsa.dev/store/migrations"
	storepostgres "eventsalsa.dev/store/postgres"

	"eventsalsa.dev/snapshot"
	snapshotpostgres "eventsalsa.dev/snapshot/postgres"
)

// User is our clean domain model.
// It has no eventsalsa dependencies or embedded types.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserCreated event payload.
type UserCreated struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserEmailChanged event payload.
type UserEmailChanged struct {
	Email string `json:"email"`
}

// UserNameChanged event payload.
type UserNameChanged struct {
	Name string `json:"name"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// Standard database connection pool.
	// Adjust connection string as needed for your local postgres setup.
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=eventsalsa_example sslmode=disable"

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to database (ensure PostgreSQL is running): %w", err)
	}
	defer db.Close()

	if setupErr := setupTables(ctx, db); setupErr != nil {
		return setupErr
	}

	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	snapshotStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	repoConfig := newUserRepoConfig(1)
	repo, err := snapshot.NewRepository(eventStore, snapshotStore, repoConfig)
	if err != nil {
		return fmt.Errorf("create snapshot repository: %w", err)
	}

	userID := uuid.New().String()

	if stepErr := stepLoadEmpty(ctx, db, repo, userID); stepErr != nil {
		return stepErr
	}

	currVer, err := stepCreateUser(ctx, db, eventStore, userID)
	if err != nil {
		return err
	}

	if appendErr := stepAppendEmailChanges(ctx, db, eventStore, userID, currVer); appendErr != nil {
		return appendErr
	}

	currVer, err = stepSaveSnapshot(ctx, db, eventStore, repo, userID)
	if err != nil {
		return err
	}

	currVer, err = stepAppendNameChanges(ctx, db, eventStore, userID, currVer)
	if err != nil {
		return err
	}

	if stepErr := stepVerifySnapshotHit(ctx, db, repo, userID, currVer); stepErr != nil {
		return stepErr
	}

	return stepSimulateSchemaMigration(ctx, db, eventStore, snapshotStore, userID)
}

func newUserRepoConfig(schemaVersion int) snapshot.RepositoryConfig[*User] {
	return snapshot.RepositoryConfig[*User]{
		StreamType:    "User",
		SchemaVersion: schemaVersion,
		Initializer: func(id string) *User {
			return &User{ID: id}
		},
		Apply: applyUserEvent,
		Codec: snapshot.JSON[*User](),
	}
}

func applyUserEvent(u *User, event store.PersistedEvent) (*User, error) { //nolint:gocritic // hugeParam: matches snapshot.RepositoryConfig Apply signature
	fmt.Printf("  [Apply Event] Replaying %s (v%d)\n", event.EventType, event.StreamVersion)
	switch event.EventType {
	case "UserCreated":
		var payload UserCreated
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return nil, err
		}
		u.Name = payload.Name
		u.Email = payload.Email
	case "UserEmailChanged":
		var payload UserEmailChanged
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return nil, err
		}
		u.Email = payload.Email
	case "UserNameChanged":
		var payload UserNameChanged
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return nil, err
		}
		u.Name = payload.Name
	}
	return u, nil
}

func stepLoadEmpty(ctx context.Context, db *pgxpool.Pool, repo *snapshot.Repository[*User], userID string) error {
	fmt.Println("\n--- Step 1: Loading non-existent User ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback on read-only tx
		_ = tx.Rollback(ctx)
	}()

	res, err := repo.Load(ctx, tx, userID)
	if err != nil {
		return fmt.Errorf("load empty user: %w", err)
	}

	fmt.Printf("Loaded state: %+v, version: %d\n", res.State, res.StreamVersion)
	return nil
}

func stepCreateUser(ctx context.Context, db *pgxpool.Pool, eventStore *storepostgres.Store, userID string) (int64, error) {
	fmt.Println("\n--- Step 2: Appending UserCreated event ---")
	payload, err := json.Marshal(UserCreated{Name: "Alice", Email: "alice@example.com"})
	if err != nil {
		return 0, fmt.Errorf("marshal UserCreated: %w", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback after commit or failure
		_ = tx.Rollback(ctx)
	}()

	events := []store.Event{
		{
			StreamType: "User",
			StreamID:   userID,
			EventID:    uuid.New(),
			EventType:  "UserCreated",
			Payload:    payload,
			CreatedAt:  time.Now(),
		},
	}
	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		return 0, fmt.Errorf("append UserCreated: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit UserCreated: %w", err)
	}

	fmt.Printf("Created User version: %d\n", result.ToVersion())
	return result.ToVersion(), nil
}

func stepAppendEmailChanges(ctx context.Context, db *pgxpool.Pool, eventStore *storepostgres.Store, userID string, currVer int64) error {
	fmt.Println("\n--- Step 3: Appending more changes (v2 -> v4) ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback after commit or failure
		_ = tx.Rollback(ctx)
	}()

	emails := []string{"alice.smith@example.com", "alice.s@example.com", "alice.s.work@example.com"}
	for _, email := range emails {
		payload, marshalErr := json.Marshal(UserEmailChanged{Email: email})
		if marshalErr != nil {
			return fmt.Errorf("marshal UserEmailChanged: %w", marshalErr)
		}
		ev := []store.Event{
			{
				StreamType: "User",
				StreamID:   userID,
				EventID:    uuid.New(),
				EventType:  "UserEmailChanged",
				Payload:    payload,
				CreatedAt:  time.Now(),
			},
		}
		res, appendErr := eventStore.Append(ctx, tx, store.Exact(currVer), ev)
		if appendErr != nil {
			return fmt.Errorf("append v%d: %w", currVer+1, appendErr)
		}
		currVer = res.ToVersion()
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit email changes: %w", err)
	}
	fmt.Printf("User is now at version: %d\n", currVer)
	return nil
}

func stepSaveSnapshot(ctx context.Context, db *pgxpool.Pool, eventStore *storepostgres.Store, repo *snapshot.Repository[*User], userID string) (int64, error) {
	fmt.Println("\n--- Step 4: Appending change (v5) and saving snapshot ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback after commit or failure
		_ = tx.Rollback(ctx)
	}()

	res, err := repo.Load(ctx, tx, userID)
	if err != nil {
		return 0, fmt.Errorf("load before snapshot: %w", err)
	}

	payload, err := json.Marshal(UserEmailChanged{Email: "alice.smith.hq@example.com"})
	if err != nil {
		return 0, fmt.Errorf("marshal UserEmailChanged: %w", err)
	}

	ev := []store.Event{
		{
			StreamType: "User",
			StreamID:   userID,
			EventID:    uuid.New(),
			EventType:  "UserEmailChanged",
			Payload:    payload,
			CreatedAt:  time.Now(),
		},
	}
	appendRes, err := eventStore.Append(ctx, tx, store.Exact(res.StreamVersion), ev)
	if err != nil {
		return 0, fmt.Errorf("append v5: %w", err)
	}

	user := res.State
	user.Email = "alice.smith.hq@example.com"
	if err := repo.Save(ctx, tx, userID, appendRes.ToVersion(), user); err != nil {
		return 0, fmt.Errorf("save snapshot: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit snapshot: %w", err)
	}

	fmt.Printf("Snapshot saved successfully at version %d with updated state: %+v\n", appendRes.ToVersion(), user)
	return appendRes.ToVersion(), nil
}

func stepAppendNameChanges(ctx context.Context, db *pgxpool.Pool, eventStore *storepostgres.Store, userID string, currVer int64) (int64, error) {
	fmt.Println("\n--- Step 5: Appending events (v6 & v7) without taking a snapshot ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback after commit or failure
		_ = tx.Rollback(ctx)
	}()

	for _, name := range []string{"Alice S.", "Alice Smith"} {
		payload, marshalErr := json.Marshal(UserNameChanged{Name: name})
		if marshalErr != nil {
			return 0, fmt.Errorf("marshal UserNameChanged: %w", marshalErr)
		}
		ev := []store.Event{
			{
				StreamType: "User",
				StreamID:   userID,
				EventID:    uuid.New(),
				EventType:  "UserNameChanged",
				Payload:    payload,
				CreatedAt:  time.Now(),
			},
		}
		resAppend, appendErr := eventStore.Append(ctx, tx, store.Exact(currVer), ev)
		if appendErr != nil {
			return 0, fmt.Errorf("append name change: %w", appendErr)
		}
		currVer = resAppend.ToVersion()
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit name changes: %w", err)
	}
	fmt.Printf("User is now at version: %d (snapshot remains at v5)\n", currVer)
	return currVer, nil
}

func stepVerifySnapshotHit(ctx context.Context, db *pgxpool.Pool, repo *snapshot.Repository[*User], userID string, expectedVer int64) error {
	fmt.Println("\n--- Step 6: Loading User (should use snapshot v5 and only replay v6 & v7) ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback on read-only tx
		_ = tx.Rollback(ctx)
	}()

	res, err := repo.Load(ctx, tx, userID)
	if err != nil {
		return fmt.Errorf("load from snapshot: %w", err)
	}

	fmt.Printf("Rehydrated state: %+v, final version: %d (snapshot hit: %v, replayed: %d, expected: %d)\n",
		res.State, res.StreamVersion, res.SnapshotHit, res.EventsReplayed, expectedVer)
	return nil
}

func stepSimulateSchemaMigration(ctx context.Context, db *pgxpool.Pool, eventStore *storepostgres.Store, snapshotStore *snapshotpostgres.Store, userID string) error {
	fmt.Println("\n--- Step 7: Simulating Schema Version Mismatch (bumping SchemaVersion to 2) ---")
	repo2, err := snapshot.NewRepository(eventStore, snapshotStore, newUserRepoConfig(2))
	if err != nil {
		return fmt.Errorf("create v2 snapshot repository: %w", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback on read-only tx
		_ = tx.Rollback(ctx)
	}()

	res, err := repo2.Load(ctx, tx, userID)
	if err != nil {
		return fmt.Errorf("load with schema v2: %w", err)
	}

	fmt.Printf("Rehydrated state (all replayed): %+v, final version: %d (snapshot hit: %v, replayed: %d)\n",
		res.State, res.StreamVersion, res.SnapshotHit, res.EventsReplayed)
	return nil
}

func setupTables(ctx context.Context, db *pgxpool.Pool) error {
	eventStoreSchema, err := storemigrations.SQL(&storemigrations.Config{})
	if err != nil {
		return fmt.Errorf("generate event store schema: %w", err)
	}

	// DDL schema for snapshots
	snapshotSchema := `
	CREATE TABLE IF NOT EXISTS snapshots (
		stream_type TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		stream_version BIGINT NOT NULL,
		schema_version INT NOT NULL,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (stream_type, stream_id, schema_version)
	);
	`

	fmt.Println("Setting up Postgres schemas if they do not exist...")

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin setup tx: %w", err)
	}
	defer func() {
		//nolint:errcheck // best-effort rollback after commit or failure
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, eventStoreSchema); err != nil {
		return fmt.Errorf("create event store tables: %w", err)
	}
	if _, err := tx.Exec(ctx, snapshotSchema); err != nil {
		return fmt.Errorf("create snapshot tables: %w", err)
	}

	// Clean up any old data for this example run
	for _, stmt := range []string{
		"TRUNCATE TABLE events CASCADE;",
		"TRUNCATE TABLE stream_heads CASCADE;",
		"TRUNCATE TABLE snapshots CASCADE;",
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("truncate table: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit setup: %w", err)
	}
	return nil
}
