package command_test

import (
	"errors"
	"fmt"
	"testing"

	"eventsalsa.dev/cqrs/command"
)

func TestRejectAndIsRejected(t *testing.T) {
	t.Parallel()

	if command.Reject(nil) != nil {
		t.Fatal("Reject(nil) must return nil")
	}
	if command.IsRejected(nil) {
		t.Fatal("IsRejected(nil) must return false")
	}

	baseErr := errors.New("insufficient balance")
	if command.IsRejected(baseErr) {
		t.Fatal("plain error must not report IsRejected")
	}

	rej := command.Reject(baseErr)
	if !command.IsRejected(rej) {
		t.Fatal("Reject(baseErr) must report IsRejected")
	}
	if !errors.Is(rej, baseErr) {
		t.Fatalf("Reject must preserve errors.Is chain: %v", rej)
	}
	if rej.Error() != baseErr.Error() {
		t.Fatalf("got Error() %q, want %q", rej.Error(), baseErr.Error())
	}

	// Idempotent double-wrapping preserves exact wrapper without stacking.
	double := command.Reject(rej)
	if !command.IsRejected(double) || !errors.Is(double, baseErr) {
		t.Fatalf("double Reject failed: %v", double)
	}
	if fmt.Sprintf("%p", double) != fmt.Sprintf("%p", rej) {
		t.Fatalf("double Reject created redundant wrapper (%p != %p)", double, rej)
	}

	// Nested wrapped or joined rejection remains detectable.
	wrapped := fmt.Errorf("reserve room: %w", rej)
	if !command.IsRejected(wrapped) {
		t.Fatal("wrapped rejection must still report IsRejected")
	}
	joined := errors.Join(errors.New("other"), rej)
	if !command.IsRejected(joined) {
		t.Fatal("joined error containing Reject must report IsRejected")
	}
}
