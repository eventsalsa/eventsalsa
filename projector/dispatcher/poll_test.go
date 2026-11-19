package dispatcher

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
)

type mockHeadQuerier struct {
	heads  []store.Cursor
	errors []error
	calls  int
	mu     sync.Mutex
}

func (m *mockHeadQuerier) GetHead(_ context.Context, _ pgx.Tx) (store.Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := m.calls
	if idx >= len(m.heads) {
		idx = len(m.heads) - 1
	}
	m.calls++

	if idx < len(m.errors) && m.errors[idx] != nil {
		return store.Cursor{}, m.errors[idx]
	}

	return m.heads[idx], nil
}

func (m *mockHeadQuerier) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.calls
}

type mockTxBeginner struct {
	beginTxErr error
	txCalls    int32
}

//nolint:gocritic // hugeParam: implements txBeginner interface
func (m *mockTxBeginner) BeginTx(_ context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	atomic.AddInt32(&m.txCalls, 1)
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	return &mockTx{}, nil
}

type mockTx struct {
	pgx.Tx
}

func (m *mockTx) Rollback(_ context.Context) error {
	return nil
}

func (m *mockTx) Commit(_ context.Context) error {
	return nil
}

func newMockDB() *mockTxBeginner {
	return &mockTxBeginner{}
}

func cur(txID uint64, pos int64) store.Cursor {
	return store.Cursor{TransactionID: txID, GlobalPosition: pos}
}

func TestPollDispatcherSendsWakeupWhenPositionAdvances(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 5), cur(10, 5), cur(10, 6)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waitForWakeup(t, dispatcher.WakeupChan())
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherSendsWakeupOnHigherXIDWithLowerPosition(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(9, 99), cur(10, 1)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waitForWakeup(t, dispatcher.WakeupChan())
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherBroadcastsWakeupToAllWaiters(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 5), cur(10, 5), cur(11, 6)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waiterDone := make(chan struct{}, 2)
	for range 2 {
		go func() {
			<-dispatcher.WakeupChan()
			waiterDone <- struct{}{}
		}()
	}

	for range 2 {
		select {
		case <-waiterDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for broadcast wakeup")
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherDoesNotSendWakeupWhenCursorUnchangedOrRegressing(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 5), cur(10, 5), cur(9, 99), cur(10, 4)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	select {
	case <-dispatcher.WakeupChan():
		cancel()
		t.Fatal("received unexpected wakeup signal")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherRespectsContextCancellation(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 2), cur(10, 2)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not return after cancellation")
	}
}

func TestPollDispatcherRejectsConcurrentStart(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 2)}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if querier.CallCount() > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Start() error = %v, want %v", err, ErrAlreadyRunning)
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherNonBlockingWakeupOnRepeatedSignals(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{
		cur(1, 1), cur(2, 2), cur(3, 3), cur(4, 4), cur(5, 5), cur(6, 6), cur(7, 7),
	}}
	dispatcher := NewPollDispatcher(newMockDB(), querier, time.Millisecond, store.NoOpLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- dispatcher.Start(ctx)
	}()

	waitForWakeup(t, dispatcher.WakeupChan())

	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if querier.CallCount() >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if got := querier.CallCount(); got < 5 {
		cancel()
		t.Fatalf("querier call count = %d, want at least 5 to prove polling continued", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

func TestPollDispatcherStartRejectsNilDB(t *testing.T) {
	dispatcher := NewPollDispatcher(nil, &mockHeadQuerier{heads: []store.Cursor{cur(1, 1)}}, time.Millisecond, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilDB) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilDB)
	}
}

func TestPollDispatcherStartRejectsNilQuerier(t *testing.T) {
	dispatcher := NewPollDispatcher(newMockDB(), nil, time.Millisecond, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilQuerier) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilQuerier)
	}
}

func TestPollDispatcherStartReturnsInitialQueryError(t *testing.T) {
	dispatcher := NewPollDispatcher(
		newMockDB(),
		&mockHeadQuerier{heads: []store.Cursor{{}}, errors: []error{errors.New("latest head failed")}},
		time.Millisecond,
		store.NoOpLogger{},
	)

	err := dispatcher.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want initialization error")
	}
	if !strings.Contains(err.Error(), "initialize poll dispatcher checkpoint: latest head failed") {
		t.Fatalf("Start() error = %v, want wrapped initialization error", err)
	}
}

func waitForWakeup(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wakeup")
	}
}
