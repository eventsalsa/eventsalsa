package application

// CancelOrder is the internal use-case input for canceling an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}
