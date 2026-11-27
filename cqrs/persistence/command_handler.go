package persistence

import (
	"context"
	"strings"

	"eventsalsa.dev/cqrs/command"
)

// NewCommandHandler adapts a pure command decision function decide onto command.Handler[C]
// backed by adapter (constructed via FromStateRepository or FromEventRepository).
//
// On every invocation, it validates ctx and target identity via idOf(cmd), loads current
// state and version through adapter, evaluates decide(state, cmd), and commits any emitted
// events with expectedVersion optimistic concurrency control. Zero-event decisions return
// nil without persisting.
func NewCommandHandler[S any, E any, C any](
	adapter Adapter[S, E],
	idOf func(cmd C) string,
	decide func(state S, cmd C) ([]E, error),
) (command.Handler[C], error) {
	if adapter.err != nil {
		return nil, adapter.err
	}
	if adapter.exec == nil {
		return nil, ErrNilAdapter
	}
	if idOf == nil {
		return nil, ErrNilTargetID
	}
	if decide == nil {
		return nil, ErrNilDecide
	}

	return command.Func[C](func(ctx context.Context, cmd C) error {
		if ctx == nil {
			return ErrNilContext
		}
		id := idOf(cmd)
		if strings.TrimSpace(id) == "" {
			return ErrEmptyTargetID
		}
		return adapter.exec(ctx, id, func(state S) ([]E, error) {
			return decide(state, cmd)
		})
	}), nil
}

// MustNewCommandHandler calls NewCommandHandler and panics if any argument is invalid.
func MustNewCommandHandler[S any, E any, C any](
	adapter Adapter[S, E],
	idOf func(cmd C) string,
	decide func(state S, cmd C) ([]E, error),
) command.Handler[C] {
	handler, err := NewCommandHandler(adapter, idOf, decide)
	if err != nil {
		panic(err)
	}
	return handler
}
