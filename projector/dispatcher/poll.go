package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"eventsalsa.dev/store"
)

// PollDispatcher checks the latest settled event cursor on a fixed interval and emits
// wakeup signals when it detects newly settled events.
type PollDispatcher struct {
	querier  HeadQuerier
	logger   store.Logger
	db       txBeginner
	wakeup   wakeupBroadcaster
	lastHead store.Cursor
	interval time.Duration
	start    startGuard
}

// NewPollDispatcher constructs a poll-based dispatcher.
func NewPollDispatcher(db txBeginner, querier HeadQuerier, interval time.Duration, logger store.Logger) *PollDispatcher {
	return &PollDispatcher{
		db:       db,
		querier:  querier,
		interval: normalizedPollInterval(interval),
		wakeup:   newWakeupBroadcaster(),
		logger:   normalizedLogger(logger),
	}
}

// Start begins the polling loop and blocks until ctx is canceled.
func (d *PollDispatcher) Start(ctx context.Context) error {
	if !d.start.tryStart() {
		return ErrAlreadyRunning
	}
	defer d.start.stop()

	if d.db == nil {
		return ErrNilDB
	}
	if d.querier == nil {
		return ErrNilQuerier
	}

	head, err := d.currentHead(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("initialize poll dispatcher checkpoint: %w", err)
	}
	d.lastHead = head

	d.logger.Debug(ctx, "poll dispatcher started",
		"interval", d.interval,
		"last_transaction_id", d.lastHead.TransactionID,
		"last_position", d.lastHead.GlobalPosition)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Debug(ctx, "poll dispatcher stopped")
			return nil
		case <-ticker.C:
			head, err := d.currentHead(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				d.logger.Error(ctx, "poll dispatcher query failed", "error", err)
				continue
			}

			if !d.lastHead.Before(head) {
				continue
			}

			d.lastHead = head
			d.signalWakeup()
			d.logger.Debug(ctx, "poll dispatcher detected new events",
				"last_transaction_id", d.lastHead.TransactionID,
				"last_position", d.lastHead.GlobalPosition)
		}
	}
}

// WakeupChan returns the wakeup channel shared with projection goroutines.
func (d *PollDispatcher) WakeupChan() <-chan struct{} {
	return d.wakeup.Channel()
}

func (d *PollDispatcher) currentHead(ctx context.Context) (store.Cursor, error) {
	tx, err := d.db.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return store.Cursor{}, ctx.Err()
		}
		return store.Cursor{}, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			d.logger.Error(ctx, "poll dispatcher head rollback failed", "error", rollbackErr)
		}
	}()

	return d.querier.GetHead(ctx, tx)
}

func (d *PollDispatcher) signalWakeup() {
	d.wakeup.Signal()
}
