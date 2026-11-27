package domain

// CancelOrder is the private domain command to cancel an existing order.
type CancelOrder struct {
	OrderID string
	Reason  string
}
