package queries

// OrderOverview is a projected read DTO queried independently of aggregate streams.
type OrderOverview struct {
	OrderID    string
	CustomerID string
	Status     string
	TotalCents int64
	Version    int64
}
