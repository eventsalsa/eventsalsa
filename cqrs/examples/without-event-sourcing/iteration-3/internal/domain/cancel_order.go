package domain

import "strings"

// CancelOrder is a private domain command decided by DecideCancelOrder.
type CancelOrder struct {
	OrderID string
	Reason  string
}

// DecideCancelOrder enforces cancellation invariants against current state and emits OrderCanceled.
func DecideCancelOrder(state Order, cmd CancelOrder) ([]any, error) {
	if state.Status == "" {
		return nil, ErrNotPlaced
	}
	if state.Status == StatusCanceled {
		return nil, ErrAlreadyCanceled
	}
	if strings.TrimSpace(cmd.Reason) == "" {
		return nil, ErrCancellationReasonRequired
	}
	return []any{OrderCanceled{Reason: cmd.Reason}}, nil
}
