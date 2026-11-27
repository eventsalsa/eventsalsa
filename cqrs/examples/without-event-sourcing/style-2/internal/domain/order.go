package domain

// Status values represent authoritative order lifecycle states.
const (
	StatusPending  = "PENDING"
	StatusCanceled = "CANCELED"
)

// Order represents authoritative write-side order state.
type Order struct {
	ID           string
	CustomerID   string
	Status       string
	CancelReason string
	TotalCents   int64
}
