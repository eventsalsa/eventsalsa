package projector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"eventsalsa.dev/store"

	"eventsalsa.dev/projector/failure"
)

func TestRegistryResolvesFailurePolicyByShape(t *testing.T) {
	tests := []struct {
		name       string
		projection any
		want       FailurePolicy
	}{
		{"transactional per-event", &stubTxProjection{name: "a"}, FailurePolicyFailFast},
		{"detached per-event", &stubDetachedProjection{name: "b"}, FailurePolicyRetry},
		{"transactional batch", &stubBatchProjection{name: "c"}, FailurePolicyFailFast},
		{"detached batch", &stubDetachedBatchProjection{name: "d"}, FailurePolicyRetry},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			if err := registry.Add(tc.projection); err != nil {
				t.Fatalf("Add() error = %v", err)
			}

			if got := registry.registrations[0].policy; got != tc.want {
				t.Fatalf("policy = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegistryFailurePolicyOverride(t *testing.T) {
	registry := NewRegistry()
	if err := registry.AddProjection(&stubTxProjection{name: "a"}, WithFailurePolicy(FailurePolicyRetry)); err != nil {
		t.Fatalf("AddProjection() error = %v", err)
	}

	if got := registry.registrations[0].policy; got != FailurePolicyRetry {
		t.Fatalf("policy = %v, want the explicit FailurePolicyRetry", got)
	}
}

func TestRegistryStoresPoisonHandler(t *testing.T) {
	handler := func(context.Context, string, []store.PersistedEvent, error) error { return nil }

	registry := NewRegistry()
	if err := registry.AddDetached(&stubDetachedProjection{name: "a"}, WithPoisonHandler(handler)); err != nil {
		t.Fatalf("AddDetached() error = %v", err)
	}

	if registry.registrations[0].poison == nil {
		t.Fatal("poison handler was not stored")
	}
}

func TestSkipPoisonBatchAdvancesCheckpoint(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	entry := registrationForTest(t, &stubDetachedProjection{name: "poison-skip"})
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  &stubProjectorStore{},
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
	}

	target := store.Cursor{TransactionID: 12, GlobalPosition: 5}
	failed := &batchError{
		cause:  failure.Permanent(errors.New("malformed document")),
		rows:   []store.PersistedEvent{unitTestEvent(1, "order")},
		target: target,
	}

	if err := daemon.skipPoisonBatch(context.Background(), entry, failed); err != nil {
		t.Fatalf("skipPoisonBatch() error = %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.execCalls != 1 {
		t.Fatalf("execCalls = %d, want 1 (checkpoint save)", state.execCalls)
	}
	if state.commitCalls != 1 {
		t.Fatalf("commitCalls = %d, want 1", state.commitCalls)
	}
	if len(state.execArgs) != 1 || len(state.execArgs[0]) != 3 {
		t.Fatalf("execArgs = %#v, want 3 checkpoint save args", state.execArgs)
	}
	if state.execArgs[0][0] != "poison-skip" || state.execArgs[0][1] != "12" || state.execArgs[0][2] != int64(5) {
		t.Fatalf("execArgs[0] = %#v, want [poison-skip 12 5]", state.execArgs[0])
	}
}

func TestSkipPoisonBatchNoOpWhenCheckpointAlreadyAhead(t *testing.T) {
	tests := []struct {
		name       string
		checkpoint store.Cursor
		target     store.Cursor
	}{
		{
			name:       "same transaction higher position",
			checkpoint: store.Cursor{TransactionID: 10, GlobalPosition: 10},
			target:     store.Cursor{TransactionID: 10, GlobalPosition: 5},
		},
		{
			name:       "higher transaction lower position",
			checkpoint: store.Cursor{TransactionID: 11, GlobalPosition: 2},
			target:     store.Cursor{TransactionID: 10, GlobalPosition: 99},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			instanceID := uuid.New()
			state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: tc.checkpoint}
			entry := registrationForTest(t, &stubDetachedProjection{name: "poison-ahead"})
			daemon := &Daemon{
				id:     instanceID,
				db:     openStubDB(t, state),
				store:  &stubProjectorStore{},
				config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
			}

			failed := &batchError{cause: failure.Permanent(errors.New("malformed")), target: tc.target}

			if err := daemon.skipPoisonBatch(context.Background(), entry, failed); err != nil {
				t.Fatalf("skipPoisonBatch() error = %v", err)
			}

			state.mu.Lock()
			defer state.mu.Unlock()
			if state.execCalls != 0 {
				t.Fatalf("execCalls = %d, want 0 when the checkpoint is already past the target", state.execCalls)
			}
			if state.commitCalls != 0 {
				t.Fatalf("commitCalls = %d, want 0 when nothing was written", state.commitCalls)
			}
		})
	}
}

func TestBatchErrorCarriesBatchAndClassification(t *testing.T) {
	inner := &batchError{
		cause:  failure.Permanent(errors.New("bad document")),
		rows:   []store.PersistedEvent{unitTestEvent(1, "order")},
		target: store.Cursor{TransactionID: 9, GlobalPosition: 3},
	}
	wrapped := fmt.Errorf("projection loop: %w", inner)

	var extracted *batchError
	if !errors.As(wrapped, &extracted) {
		t.Fatal("errors.As did not find the batchError")
	}
	if extracted.target != (store.Cursor{TransactionID: 9, GlobalPosition: 3}) || len(extracted.rows) != 1 {
		t.Fatalf("extracted = %#v, want target {9, 3} and one event", extracted)
	}
	if !failure.IsPermanent(wrapped) {
		t.Fatal("wrapped permanent batch error is not reported as permanent")
	}
	if !errors.Is(wrapped, inner.cause) {
		t.Fatal("batchError does not unwrap to its cause")
	}
}
