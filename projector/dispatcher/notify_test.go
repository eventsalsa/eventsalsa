package dispatcher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"eventsalsa.dev/store"
)

func TestNotifyDispatcherReconcileSendsWakeupWhenPositionAdvances(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 6)}}
	dispatcher := NewNotifyDispatcher("", "worker_events", newMockDB(), querier, store.NoOpLogger{})
	dispatcher.lastHead = cur(10, 5)

	// Capture channel before reconcile so we observe the close-and-replace signal.
	ch := dispatcher.WakeupChan()

	if err := dispatcher.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}

	waitForWakeup(t, ch)
}

func TestNotifyDispatcherReconcileSendsWakeupWhenHigherXIDHasLowerPosition(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 1)}}
	dispatcher := NewNotifyDispatcher("", "worker_events", newMockDB(), querier, store.NoOpLogger{})
	dispatcher.lastHead = cur(9, 99)

	ch := dispatcher.WakeupChan()

	if err := dispatcher.reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile() error = %v", err)
	}

	waitForWakeup(t, ch)
}

func TestNotifyDispatcherRejectsConcurrentStart(t *testing.T) {
	querier := &mockHeadQuerier{heads: []store.Cursor{cur(10, 3)}}
	dispatcher := NewNotifyDispatcher("", "worker_events", newMockDB(), querier, store.NoOpLogger{})

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

func TestNotifyDispatcherStartRejectsNilDB(t *testing.T) {
	dispatcher := NewNotifyDispatcher("", "worker_events", nil, &mockHeadQuerier{heads: []store.Cursor{cur(1, 1)}}, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrNilDB) {
		t.Fatalf("Start() error = %v, want %v", err, ErrNilDB)
	}
}

func TestNotifyDispatcherStartRejectsEmptyChannel(t *testing.T) {
	dispatcher := NewNotifyDispatcher("", "", newMockDB(), &mockHeadQuerier{heads: []store.Cursor{cur(1, 1)}}, store.NoOpLogger{})

	if err := dispatcher.Start(context.Background()); !errors.Is(err, ErrEmptyChannel) {
		t.Fatalf("Start() error = %v, want %v", err, ErrEmptyChannel)
	}
}

func TestNotifyDispatcherStartReturnsInitialQueryError(t *testing.T) {
	dispatcher := NewNotifyDispatcher(
		"",
		"worker_events",
		newMockDB(),
		&mockHeadQuerier{heads: []store.Cursor{{}}, errors: []error{errors.New("latest head failed")}},
		store.NoOpLogger{},
	)

	err := dispatcher.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want initialization error")
	}
	if !strings.Contains(err.Error(), "initialize notify dispatcher checkpoint: latest head failed") {
		t.Fatalf("Start() error = %v, want wrapped initialization error", err)
	}
}

func TestNotifyDispatcherReconcileDoesNotWakeWhenCursorUnchangedOrRegressing(t *testing.T) {
	for _, head := range []store.Cursor{cur(10, 5), cur(9, 99), cur(10, 4)} {
		dispatcher := NewNotifyDispatcher("", "worker_events", newMockDB(), &mockHeadQuerier{heads: []store.Cursor{head}}, store.NoOpLogger{})
		dispatcher.lastHead = cur(10, 5)

		ch := dispatcher.WakeupChan()
		if err := dispatcher.reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile() error = %v", err)
		}

		select {
		case <-ch:
			t.Fatalf("received unexpected wakeup signal for head %+v", head)
		default:
		}

		if dispatcher.WakeupChan() != ch {
			t.Fatalf("wakeup channel changed without a cursor advance for head %+v", head)
		}
	}
}
