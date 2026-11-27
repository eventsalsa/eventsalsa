package domain

// OrderPlaced records valid creation of a customer order.
type OrderPlaced struct {
	OrderID    string
	CustomerID string
	TotalCents int64
}

// OrderCanceled records cancellation of an order with its stated reason.
type OrderCanceled struct {
	Reason string
}
