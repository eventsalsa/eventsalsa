package cqrs_test

import (
	"errors"
	"fmt"
	"testing"

	"eventsalsa.dev/cqrs"
)

func TestRejectAndIsRejected(t *testing.T) {
	t.Parallel()

	if cqrs.Reject(nil) != nil {
		t.Fatal("Reject(nil) must return nil")
	}
	if cqrs.IsRejected(nil) {
		t.Fatal("IsRejected(nil) must return false")
	}

	baseErr := errors.New("insufficient balance")
	if cqrs.IsRejected(baseErr) {
		t.Fatal("plain error must not report IsRejected")
	}

	rej := cqrs.Reject(baseErr)
	if !cqrs.IsRejected(rej) {
		t.Fatal("Reject(baseErr) must report IsRejected")
	}
	if !errors.Is(rej, baseErr) {
		t.Fatalf("Reject must preserve errors.Is chain: %v", rej)
	}
	if rej.Error() != baseErr.Error() {
		t.Fatalf("got Error() %q, want %q", rej.Error(), baseErr.Error())
	}

	// Idempotent double-wrapping preserves exact wrapper without stacking.
	double := cqrs.Reject(rej)
	if !cqrs.IsRejected(double) || !errors.Is(double, baseErr) {
		t.Fatalf("double Reject failed: %v", double)
	}
	if fmt.Sprintf("%p", double) != fmt.Sprintf("%p", rej) {
		t.Fatalf("double Reject created redundant wrapper (%p != %p)", double, rej)
	}

	// Nested wrapped or joined rejection remains detectable.
	wrapped := fmt.Errorf("reserve room: %w", rej)
	if !cqrs.IsRejected(wrapped) {
		t.Fatal("wrapped rejection must still report IsRejected")
	}
	joined := errors.Join(errors.New("other"), rej)
	if !cqrs.IsRejected(joined) {
		t.Fatal("joined error containing Reject must report IsRejected")
	}
}
