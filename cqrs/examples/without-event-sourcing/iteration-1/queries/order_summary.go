package queries

// OrderSummary is a UI/API-ready projection DTO shaped for callers.
type OrderSummary struct {
	OrderID      string
	CustomerName string
	Status       string
	DisplayTotal string
}
