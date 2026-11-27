package commands

// CancelOrder requests canceling an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}
