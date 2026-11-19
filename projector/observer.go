package projector

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// BatchStats contains execution telemetry for a single projection batch.
type BatchStats struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	ProjectionName     string
	StartTransactionID uint64        // Checkpoint transaction ID prior to batch execution
	StartPosition      int64         // Checkpoint position prior to batch execution
	LastTransactionID  uint64        // Checkpoint transaction ID after batch execution
	LastPosition       int64         // Checkpoint position after batch execution
	HeadTransactionID  uint64        // Settled log head transaction ID
	HeadPosition       int64         // Settled log head position
	Lag                int64         // Remaining settled events behind HeadPosition
	EventsRead         int           // Total events read in the batch window
	EventsHandled      int           // Events applied by the projection after registration filtering
	Duration           time.Duration // Total batch duration (probe + handle + checkpoint commit)
	Detached           bool          // True if the projection was applied outside the daemon transaction
	Error              error         // Non-nil if batch execution failed
}

// DaemonStats contains periodic daemon health and lease telemetry.
type DaemonStats struct {
	InstanceID uuid.UUID
	IsLeader   bool
}

// DegradedStats captures a batch failure handled without stopping the daemon,
// such as a Retryable error or a failure under FailurePolicyRetry.
type DegradedStats struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	ProjectionName      string
	ConsecutiveFailures int
	FirstFailureAt      time.Time
	LastError           error
}

// PoisonBatchStats is reported after a Permanent failure has been recorded by a
// poison handler and the checkpoint has advanced past the batch.
type PoisonBatchStats struct { //nolint:govet // fieldalignment: readability over marginal memory savings
	ProjectionName      string
	TargetTransactionID uint64
	TargetPosition      int64
	EventCount          int
	Cause               error
}

// Observer receives runtime telemetry and lifecycle callbacks from a projector Daemon.
// All callbacks are executed synchronously on the emitting goroutine; implementations must be
// thread-safe and return promptly so they do not stall projection progress.
type Observer interface {
	// OnBatchProcessed is called after every batch processing attempt (success or error).
	//nolint:gocritic // hugeParam: pass by value for immutable snapshot
	OnBatchProcessed(ctx context.Context, stats BatchStats)

	// OnHeartbeat is called after each successful heartbeat refresh.
	OnHeartbeat(ctx context.Context, stats DaemonStats)

	// OnProjectionDegraded is called when a projection fails without the failure
	// being fatal, so operators can alert on sustained degradation.
	OnProjectionDegraded(ctx context.Context, stats DegradedStats)

	// OnPoisonBatchSkipped is called after a Permanent failure is recorded by a
	// poison handler and the checkpoint advances past the batch.
	OnPoisonBatchSkipped(ctx context.Context, stats PoisonBatchStats)

	// OnRebalance is called when projection partition assignments are updated.
	OnRebalance(ctx context.Context, assignments map[string]uuid.UUID)
}

// NoopObserver is an embeddable Observer implementation whose methods do nothing.
// Custom observers can embed NoopObserver to implement only the callbacks they care about.
type NoopObserver struct{}

// OnBatchProcessed is a no-op implementation.
//
//nolint:gocritic // hugeParam: pass by value for immutable snapshot
func (NoopObserver) OnBatchProcessed(context.Context, BatchStats) {}

// OnHeartbeat is a no-op implementation.
func (NoopObserver) OnHeartbeat(context.Context, DaemonStats) {}

// OnProjectionDegraded is a no-op implementation.
func (NoopObserver) OnProjectionDegraded(context.Context, DegradedStats) {}

// OnPoisonBatchSkipped is a no-op implementation.
func (NoopObserver) OnPoisonBatchSkipped(context.Context, PoisonBatchStats) {}

// OnRebalance is a no-op implementation.
func (NoopObserver) OnRebalance(context.Context, map[string]uuid.UUID) {}

// MultiObserver combines multiple Observer implementations so every callback is forwarded
// to all non-nil observers in order.
// Returns NoopObserver when no non-nil observers are provided.
func MultiObserver(observers ...Observer) Observer {
	filtered := make([]Observer, 0, len(observers))
	for _, o := range observers {
		if o == nil {
			continue
		}
		if m, ok := o.(multiObserver); ok {
			filtered = append(filtered, m...)
		} else {
			filtered = append(filtered, o)
		}
	}

	switch len(filtered) {
	case 0:
		return NoopObserver{}
	case 1:
		return filtered[0]
	default:
		return multiObserver(filtered)
	}
}

type multiObserver []Observer

//nolint:gocritic // hugeParam: pass by value for immutable snapshot
func (mo multiObserver) OnBatchProcessed(ctx context.Context, stats BatchStats) {
	for _, o := range mo {
		o.OnBatchProcessed(ctx, stats)
	}
}

func (mo multiObserver) OnHeartbeat(ctx context.Context, stats DaemonStats) {
	for _, o := range mo {
		o.OnHeartbeat(ctx, stats)
	}
}

func (mo multiObserver) OnProjectionDegraded(ctx context.Context, stats DegradedStats) {
	for _, o := range mo {
		o.OnProjectionDegraded(ctx, stats)
	}
}

func (mo multiObserver) OnPoisonBatchSkipped(ctx context.Context, stats PoisonBatchStats) {
	for _, o := range mo {
		o.OnPoisonBatchSkipped(ctx, stats)
	}
}

func (mo multiObserver) OnRebalance(ctx context.Context, assignments map[string]uuid.UUID) {
	for _, o := range mo {
		o.OnRebalance(ctx, assignments)
	}
}
