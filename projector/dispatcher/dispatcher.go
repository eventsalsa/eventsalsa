package dispatcher

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
)

const (
	defaultPollInterval        = 200 * time.Millisecond
	defaultNotifyPollFallback  = time.Second
	defaultListenerMinInterval = 10 * time.Second
	defaultListenerMaxInterval = time.Minute
)

var (
	// ErrNilDB indicates that a dispatcher was created without a database handle.
	ErrNilDB = errors.New("dispatcher requires a database handle")

	// ErrNilQuerier indicates that a dispatcher was created without a head querier.
	ErrNilQuerier = errors.New("dispatcher requires a head querier")

	// ErrEmptyChannel indicates that a notify dispatcher was created without a channel name.
	ErrEmptyChannel = errors.New("dispatcher requires a notification channel")

	// ErrAlreadyRunning indicates that Start was called while the dispatcher was already running.
	ErrAlreadyRunning = errors.New("dispatcher already running")
)

// Dispatcher detects new events and broadcasts wakeup signals to projection goroutines.
// Implementations must be safe for concurrent use.
type Dispatcher interface {
	// Start begins the dispatcher's detection loop. Blocks until ctx is canceled.
	// The wakeup channel is broadcast by closing the current channel and replacing it
	// with a new one each time new events are detected.
	Start(ctx context.Context) error

	// WakeupChan returns the current wakeup generation channel. Projections should call
	// WakeupChan each time they enter a select so they observe future broadcasts
	// after the current generation is closed.
	WakeupChan() <-chan struct{}
}

// txBeginner abstracts the ability to start transactions.
type txBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// HeadQuerier queries the latest settled event cursor behind PostgreSQL's active
// transaction visibility horizon.
type HeadQuerier interface {
	// GetHead returns the highest (TransactionID, GlobalPosition) cursor visible behind
	// pg_snapshot_xmin(pg_current_snapshot()), or the zero store.Cursor if none exist.
	GetHead(ctx context.Context, tx pgx.Tx) (store.Cursor, error)
}

func normalizedPollInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return defaultPollInterval
	}

	return interval
}

func normalizedLogger(logger store.Logger) store.Logger {
	if logger == nil {
		return store.NoOpLogger{}
	}

	return logger
}

type wakeupBroadcaster struct {
	ch chan struct{}
	mu sync.RWMutex
}

func newWakeupBroadcaster() wakeupBroadcaster {
	return wakeupBroadcaster{
		ch: make(chan struct{}),
	}
}

func (b *wakeupBroadcaster) Channel() <-chan struct{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.ch
}

func (b *wakeupBroadcaster) Signal() {
	b.mu.Lock()
	defer b.mu.Unlock()

	current := b.ch
	b.ch = make(chan struct{})
	close(current)
}

type startGuard struct {
	mu      sync.Mutex
	running bool
}

func (g *startGuard) tryStart() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		return false
	}

	g.running = true
	return true
}

func (g *startGuard) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.running = false
}
