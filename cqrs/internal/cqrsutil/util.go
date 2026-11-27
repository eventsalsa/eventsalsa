// Package cqrsutil provides shared internal reflection, middleware-chaining, and
// ID-generation helpers for the eventsalsa.dev/cqrs module.
package cqrsutil

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

// Chain composes non-nil middlewares in outermost-first execution order.
func Chain[M ~func(F) F, F any](mws ...M) M {
	filtered := make([]M, 0, len(mws))
	for _, mw := range mws {
		if mw != nil {
			filtered = append(filtered, mw)
		}
	}
	return func(next F) F {
		wrapped := next
		for i := len(filtered) - 1; i >= 0; i-- {
			wrapped = filtered[i](wrapped)
		}
		return wrapped
	}
}

// ConcreteMessageType validates that T is not an interface type, returning invalidErr otherwise.
func ConcreteMessageType[T any](invalidErr error) (reflect.Type, error) {
	rt := reflect.TypeFor[T]()
	if rt.Kind() == reflect.Interface {
		return nil, invalidErr
	}
	return rt, nil
}

// IsNilValue reports whether v is nil or a typed-nil value of a nillable kind.
func IsNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// IsNilMessage reports whether v is nil or a typed-nil interface/pointer value.
func IsNilMessage(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer:
		return rv.IsNil()
	default:
		return false
	}
}

// NewIDGenerator returns a generator backed by fn when non-nil, falling back to
// uuid.NewString if fn is nil or returns an empty string.
func NewIDGenerator(fn func() string) func() string {
	return func() string {
		if fn != nil {
			if id := fn(); id != "" {
				return id
			}
		}
		return uuid.NewString()
	}
}

// ConvertResult casts raw middleware/handler output into R or wraps mismatchErr.
func ConvertResult[R any](raw any, mismatchErr error) (R, error) {
	var zero R
	if raw == nil {
		return zero, nil
	}
	typed, ok := raw.(R)
	if !ok {
		return zero, fmt.Errorf("%w: got %T, want %T", mismatchErr, raw, zero)
	}
	return typed, nil
}
