package domain

import "strings"

// CancelOrder is the private domain command to cancel an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}

// HandleCancelOrder is a pure, side-effect-free domain handler that validates
// cancellation invariants and returns an updated Order value.
func HandleCancelOrder(order Order, cmd CancelOrder) (Order, error) {
	if order.Status == StatusCanceled {
		return Order{}, ErrAlreadyCanceled
	}
	if strings.TrimSpace(cmd.Reason) == "" {
		return Order{}, ErrCancellationReasonRequired
	}
	order.Status = StatusCanceled
	order.CancelReason = cmd.Reason
	return order, nil
}
