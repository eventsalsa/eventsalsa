package cqrs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"eventsalsa.dev/cqrs"
)

func TestRetryCommandMiddleware_SuccessAndRetries(t *testing.T) {
	t.Parallel()

	type workCmd struct{}

	errTransient := errors.New("transient failure")
	errFatal := errors.New("fatal failure")

	t.Run("succeeds after matching transient errors", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[workCmd](func(_ context.Context, _ workCmd) error {
			attempts++
			if attempts < 3 {
				return errTransient
			}
			return nil
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 3,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  5 * time.Millisecond,
			RetryIf:     func(e error) bool { return errors.Is(e, errTransient) },
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		if runErr := h.Handle(context.Background(), workCmd{}); runErr != nil {
			t.Fatalf("expected eventual success, got %v", runErr)
		}
		if attempts != 3 {
			t.Fatalf("got %d attempts, want 3", attempts)
		}
	})

	t.Run("stops immediately on non-matching error", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[workCmd](func(_ context.Context, _ workCmd) error {
			attempts++
			return errFatal
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 4,
			BaseBackoff: time.Millisecond,
			RetryIf:     func(e error) bool { return errors.Is(e, errTransient) },
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		runErr := h.Handle(context.Background(), workCmd{})
		if !errors.Is(runErr, errFatal) {
			t.Fatalf("got %v, want %v", runErr, errFatal)
		}
		if attempts != 1 {
			t.Fatalf("expected 1 attempt on non-retryable err, got %d", attempts)
		}
	})

	t.Run("exhausts MaxAttempts and returns final error", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[workCmd](func(_ context.Context, _ workCmd) error {
			attempts++
			return errTransient
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 3,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  2 * time.Millisecond,
			RetryIf:     func(e error) bool { return errors.Is(e, errTransient) },
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		runErr := h.Handle(context.Background(), workCmd{})
		if !errors.Is(runErr, errTransient) || attempts != 3 {
			t.Fatalf("got err=%v attempts=%d, want errTransient and 3 attempts", runErr, attempts)
		}
	})

	t.Run("nil RetryIf and <=1 MaxAttempts never retry", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[workCmd](func(_ context.Context, _ workCmd) error {
			attempts++
			return errTransient
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 5,
			RetryIf:     nil,
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		runErr := h.Handle(context.Background(), workCmd{})
		if !errors.Is(runErr, errTransient) || attempts != 1 {
			t.Fatalf("nil RetryIf must run once, got err=%v attempts=%d", runErr, attempts)
		}
	})
}

func TestRetryCommandMiddleware_ContextCancellationAndCausationStability(t *testing.T) {
	t.Parallel()

	type retryCmd struct{}

	errRetry := errors.New("retryable")

	t.Run("pre-canceled context aborts before attempt", func(t *testing.T) {
		t.Parallel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[retryCmd](func(_ context.Context, _ retryCmd) error {
			attempts++
			return nil
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 3,
			RetryIf:     func(error) bool { return true },
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runErr := h.Handle(ctx, retryCmd{})
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", runErr)
		}
		if attempts != 0 {
			t.Fatalf("expected 0 handler invocations on canceled ctx, got %d", attempts)
		}
	})

	t.Run("cancellation during backoff sleep exits promptly", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		attempts := 0
		h, err := cqrs.WrapCommand(cqrs.CommandFunc[retryCmd](func(_ context.Context, _ retryCmd) error {
			attempts++
			cancel()
			return errRetry
		}), cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
			MaxAttempts: 4,
			BaseBackoff: 500 * time.Millisecond,
			MaxBackoff:  time.Second,
			RetryIf:     func(error) bool { return true },
		}))
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		start := time.Now()
		runErr := h.Handle(ctx, retryCmd{})
		elapsed := time.Since(start)
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", runErr)
		}
		if attempts != 1 || elapsed > 200*time.Millisecond {
			t.Fatalf("expected immediate cancellation exit, got attempts=%d elapsed=%v", attempts, elapsed)
		}
	})

	t.Run("CausationMiddleware outside Retry preserves single CommandID", func(t *testing.T) {
		t.Parallel()

		var seenCommandIDs []string
		h, err := cqrs.WrapCommand(
			cqrs.CommandFunc[retryCmd](func(ctx context.Context, _ retryCmd) error {
				seenCommandIDs = append(seenCommandIDs, cqrs.CommandIDFromContext(ctx))
				if len(seenCommandIDs) < 2 {
					return errRetry
				}
				return nil
			}),
			cqrs.CausationMiddleware(),
			cqrs.RetryCommandMiddleware(cqrs.RetryConfig{
				MaxAttempts: 3,
				BaseBackoff: time.Millisecond,
				MaxBackoff:  5 * time.Millisecond,
				RetryIf:     func(e error) bool { return errors.Is(e, errRetry) },
			}),
		)
		if err != nil {
			t.Fatalf("WrapCommand: %v", err)
		}

		if runErr := h.Handle(context.Background(), retryCmd{}); runErr != nil {
			t.Fatalf("Handle: %v", runErr)
		}
		if len(seenCommandIDs) != 2 || seenCommandIDs[0] == "" || seenCommandIDs[0] != seenCommandIDs[1] {
			t.Fatalf("expected stable CommandID across retries, got %v", seenCommandIDs)
		}
	})
}
