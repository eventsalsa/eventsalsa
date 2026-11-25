package cqrs

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	defaultBaseBackoff = 10 * time.Millisecond
	defaultMaxBackoff  = 250 * time.Millisecond
)

// RetryConfig configures jittered exponential backoff in RetryCommandMiddleware.
type RetryConfig struct {
	// RetryIf decides whether err triggers another attempt. Nil disables retries (fail-safe).
	RetryIf func(error) bool

	// MaxAttempts sets the total number of attempts (values <= 1 disable retries).
	MaxAttempts int

	// BaseBackoff sets the initial jitter ceiling between attempts 1 and 2.
	// Defaults to min(10ms, MaxBackoff) when MaxAttempts > 1 and BaseBackoff <= 0.
	BaseBackoff time.Duration

	// MaxBackoff caps computed backoff delays (defaults to 250ms when <= 0).
	MaxBackoff time.Duration
}

// RetryCommandMiddleware wraps command execution with full-jitter exponential backoff retries
// whenever cfg.RetryIf returns true. Respects ctx cancellation before attempts and during sleeps.
func RetryCommandMiddleware(cfg RetryConfig) CommandMiddleware {
	maxAttempts, retryIf, base, maxBackoff := normalizeRetryParams(cfg)

	return func(next UntypedCommandFunc) UntypedCommandFunc {
		return func(ctx context.Context, cmd any) error {
			ctx = normalizeContext(ctx)
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				err := next(ctx, cmd)
				if err == nil || attempt == maxAttempts || !retryIf(err) {
					return err
				}
				if waitErr := waitRetryDelay(ctx, jitteredDelay(attempt, base, maxBackoff)); waitErr != nil {
					return waitErr
				}
			}
			return nil
		}
	}
}

func normalizeRetryParams(cfg RetryConfig) (maxAttempts int, retryIf func(error) bool, base, maxBackoff time.Duration) {
	if cfg.RetryIf == nil || cfg.MaxAttempts <= 1 {
		return 1, func(error) bool { return false }, 0, 0
	}
	maxAttempts = cfg.MaxAttempts
	retryIf = cfg.RetryIf

	maxBackoff = cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}
	base = cfg.BaseBackoff
	if base <= 0 {
		base = min(defaultBaseBackoff, maxBackoff)
	} else if maxBackoff < base {
		maxBackoff = base
	}
	return maxAttempts, retryIf, base, maxBackoff
}

func waitRetryDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func jitteredDelay(attempt int, base, maxBackoff time.Duration) time.Duration {
	shift := max(attempt-1, 0)
	if shift > 30 {
		shift = 30
	}

	ceiling := base
	for i := 0; i < shift; i++ {
		if ceiling >= maxBackoff/2 {
			ceiling = maxBackoff
			break
		}
		ceiling *= 2
	}
	if ceiling <= 0 {
		return 0
	}
	// #nosec G404 -- backoff jitter is non-cryptographic
	return time.Duration(rand.Int64N(int64(ceiling) + 1)) //nolint:gosec // G404: backoff jitter is non-cryptographic
}
