package persistence

import (
	"context"
	"errors"
)

// ErrNotFound indicates no persisted state row exists yet for the requested identity.
var ErrNotFound = errors.New("state not found")

// StateRepository persists aggregate decision state without an event log.
type StateRepository[S any] interface {
	Load(ctx context.Context, id string) (S, error)
	Save(ctx context.Context, state S) error
}
