package events

// OrderItemAdded records adding quantity units of a SKU to a placed order.
type OrderItemAdded struct {
	SKU       string `json:"sku"`
	UnitCents int64  `json:"unit_cents"`
	Quantity  int64  `json:"quantity"`
}
