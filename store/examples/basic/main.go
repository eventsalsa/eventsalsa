// Package main demonstrates basic usage of the eventsalsa event store.
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
	"eventsalsa.dev/store/postgres"
)

//go:generate go run ../../cmd/migrate-gen -output ../../migrations -filename init.sql

// UserCreated is a sample event payload.
type UserCreated struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

// UserEmailChanged is a sample event payload.
type UserEmailChanged struct {
	OldEmail string `json:"old_email"`
	NewEmail string `json:"new_email"`
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// #nosec G101 -- local development example default DSN
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=eventsalsa_example sslmode=disable" //nolint:gosec // G101: local development example default DSN

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer db.Close()

	eventStore := postgres.NewStore(postgres.DefaultStoreConfig())
	userID := uuid.New().String()

	result, err := appendUserCreated(ctx, db, eventStore, userID)
	if err != nil {
		return err
	}

	if appendErr := appendEmailChanged(ctx, db, eventStore, userID, result.ToVersion()); appendErr != nil {
		return appendErr
	}

	if printErr := printStream(ctx, db, eventStore, userID); printErr != nil {
		return printErr
	}

	return printAllEvents(ctx, db, eventStore)
}

func appendUserCreated(ctx context.Context, db *pgxpool.Pool, eventStore *postgres.Store, userID string) (store.AppendResult, error) {
	fmt.Println("--- Example 1: Create New Stream ---")

	payload, err := json.Marshal(UserCreated{
		Email: "alice@example.com",
		Name:  "Alice Smith",
	})
	if err != nil {
		return store.AppendResult{}, fmt.Errorf("marshal UserCreated: %w", err)
	}

	events := []store.Event{
		{
			StreamType:   "User",
			StreamID:     userID,
			EventID:      uuid.New(),
			EventType:    "UserCreated",
			EventVersion: 1,
			Payload:      payload,
			Metadata:     []byte(`{}`),
			CreatedAt:    time.Now(),
		},
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return store.AppendResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		_ = tx.Rollback(ctx)
	}()

	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		return store.AppendResult{}, fmt.Errorf("append events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return store.AppendResult{}, fmt.Errorf("commit transaction: %w", err)
	}

	fmt.Printf("Events appended at positions: %v\n", result.GlobalPositions)
	fmt.Printf("Stream is now at version: %d\n", result.ToVersion())
	return result, nil
}

func appendEmailChanged(ctx context.Context, db *pgxpool.Pool, eventStore *postgres.Store, userID string, prevVersion int64) error {
	fmt.Println("\n--- Example 2: Append with Optimistic Concurrency ---")

	payload, err := json.Marshal(UserEmailChanged{
		OldEmail: "alice@example.com",
		NewEmail: "alice.smith@example.com",
	})
	if err != nil {
		return fmt.Errorf("marshal UserEmailChanged: %w", err)
	}

	events := []store.Event{
		{
			StreamType:   "User",
			StreamID:     userID,
			EventID:      uuid.New(),
			EventType:    "UserEmailChanged",
			EventVersion: 1,
			Payload:      payload,
			Metadata:     []byte(`{}`),
			CreatedAt:    time.Now(),
		},
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		_ = tx.Rollback(ctx)
	}()

	result, err := eventStore.Append(ctx, tx, store.Exact(prevVersion), events)
	if err != nil {
		return fmt.Errorf("append events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	fmt.Printf("Events appended. Version: %d → %d\n", prevVersion, result.ToVersion())
	return nil
}

func printStream(ctx context.Context, db *pgxpool.Pool, eventStore *postgres.Store, userID string) error {
	fmt.Println("\n--- Example 3: Read Stream ---")

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		_ = tx.Rollback(ctx)
	}()

	stream, err := eventStore.ReadStream(ctx, tx, "User", userID, nil, nil)
	if err != nil {
		return fmt.Errorf("read stream: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	fmt.Printf("Stream: %d events, version %d\n", stream.Len(), stream.Version())
	for i := range stream.Events {
		event := &stream.Events[i]
		fmt.Printf("  Event %d: %s (v%d) at position %d (tx %d)\n",
			i+1, event.EventType, event.StreamVersion, event.GlobalPosition, event.TransactionID)
	}
	return nil
}

func printAllEvents(ctx context.Context, db *pgxpool.Pool, eventStore *postgres.Store) error {
	fmt.Println("\n--- Example 4: Read Events Sequentially ---")

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		//nolint:errcheck // Rollback error ignored: expected to fail if commit succeeds
		_ = tx.Rollback(ctx)
	}()

	allEvents, err := eventStore.ReadEvents(ctx, tx, store.Cursor{}, 100)
	if err != nil {
		return fmt.Errorf("read events: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	fmt.Printf("Read %d events from the global log\n", len(allEvents))
	for i := range allEvents {
		event := &allEvents[i]
		fmt.Printf("  Position %d: %s/%s.%s (v%d)\n",
			event.GlobalPosition, event.StreamType, event.StreamID[:8], event.EventType, event.StreamVersion)
	}
	return nil
}
