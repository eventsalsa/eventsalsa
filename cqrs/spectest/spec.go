// Package spectest provides a zero-database, sub-millisecond BDD Given/When/Then
// specification harness for testing pure Go event-sourced decision and evolution functions.
package spectest

import (
	"errors"
	"reflect"
	"testing"
)

// Spec defines the initial state factory and evolution function for a stream specification.
type Spec[S any, E any] struct {
	t       testing.TB
	initial func(streamID string) S
	evolve  func(state S, event E) S
}

// GivenSpec holds historical events folded prior to executing a command under test.
type GivenSpec[S any, E any] struct {
	spec   *Spec[S, E]
	events []E
}

// Outcome captures the emitted events, folded post-command state, or returned error
// from executing When, exposing fluent assertions.
type Outcome[S any, E any] struct {
	t      testing.TB
	err    error
	state  S
	events []E
}

// For creates a new specification harness for state type S and event type E.
func For[S any, E any](
	t testing.TB,
	initial func(streamID string) S,
	evolve func(state S, event E) S,
) *Spec[S, E] {
	if t == nil {
		panic("spectest: testing.TB cannot be nil")
	}
	t.Helper()
	if initial == nil {
		t.Fatal("spectest: initial func cannot be nil")
	}
	if evolve == nil {
		t.Fatal("spectest: evolve func cannot be nil")
	}

	return &Spec[S, E]{
		t:       t,
		initial: initial,
		evolve:  evolve,
	}
}

// Given prepares historical events that are folded before running When.
func (s *Spec[S, E]) Given(events ...E) *GivenSpec[S, E] {
	return &GivenSpec[S, E]{
		spec:   s,
		events: events,
	}
}

// When runs a typed command function against an empty stream (no Given history) using
// Go 1.27 generic methods, returning an Outcome for assertion.
func (s *Spec[S, E]) When[C any](
	streamID string,
	cmd C,
	decide func(state S, cmd C) ([]E, error),
) *Outcome[S, E] {
	s.t.Helper()
	return s.Given().When(streamID, cmd, decide)
}

// When folds Given events from Initial(streamID), executes decide(state, cmd), and
// folds any newly emitted events into the resulting state using Go 1.27 generic methods.
func (g *GivenSpec[S, E]) When[C any](
	streamID string,
	cmd C,
	decide func(state S, cmd C) ([]E, error),
) *Outcome[S, E] {
	g.spec.t.Helper()
	if decide == nil {
		g.spec.t.Fatal("spectest: decide func cannot be nil")
	}

	state := g.spec.initial(streamID)
	for i := range g.events {
		state = g.spec.evolve(state, g.events[i])
	}

	emitted, err := decide(state, cmd)
	if err == nil {
		for i := range emitted {
			state = g.spec.evolve(state, emitted[i])
		}
	}

	return &Outcome[S, E]{
		t:      g.spec.t,
		events: emitted,
		state:  state,
		err:    err,
	}
}

// Then asserts that When succeeded and emitted the exact events in want (or zero events when want is empty).
func (o *Outcome[S, E]) Then(want ...E) *Outcome[S, E] {
	o.t.Helper()
	if o.err != nil {
		o.t.Fatalf("spectest: unexpected error: %v", o.err)
	}
	if len(want) == 0 {
		if len(o.events) != 0 {
			o.t.Fatalf("spectest: expected no events, got %#v", o.events)
		}
		return o
	}
	if !reflect.DeepEqual(o.events, want) {
		o.t.Fatalf("spectest: events mismatch\n got: %#v\nwant: %#v", o.events, want)
	}
	return o
}

// ThenNoEvents asserts that When succeeded without emitting any events.
func (o *Outcome[S, E]) ThenNoEvents() *Outcome[S, E] {
	o.t.Helper()
	if o.err != nil {
		o.t.Fatalf("spectest: unexpected error: %v", o.err)
	}
	if len(o.events) != 0 {
		o.t.Fatalf("spectest: expected no events, got %#v", o.events)
	}
	return o
}

// ThenState asserts that When succeeded and runs check against the post-command state.
func (o *Outcome[S, E]) ThenState(check func(state S)) *Outcome[S, E] {
	o.t.Helper()
	if o.err != nil {
		o.t.Fatalf("spectest: unexpected error: %v", o.err)
	}
	if check == nil {
		o.t.Fatal("spectest: ThenState check func cannot be nil")
	}
	check(o.state)
	return o
}

// ThenError asserts that When returned a non-nil error.
func (o *Outcome[S, E]) ThenError() *Outcome[S, E] {
	o.t.Helper()
	if o.err == nil {
		o.t.Fatalf("spectest: expected an error, got nil (events: %#v)", o.events)
	}
	return o
}

// ThenErrorIs asserts that When returned an error matching target via errors.Is.
func (o *Outcome[S, E]) ThenErrorIs(target error) *Outcome[S, E] {
	o.t.Helper()
	if o.err == nil {
		o.t.Fatalf("spectest: expected error matching %v, got nil (events: %#v)", target, o.events)
	}
	if !errors.Is(o.err, target) {
		o.t.Fatalf("spectest: error %v does not match %v", o.err, target)
	}
	return o
}

// ThenErrorCheck asserts that When returned a non-nil error and passes it to check.
func (o *Outcome[S, E]) ThenErrorCheck(check func(err error)) *Outcome[S, E] {
	o.t.Helper()
	if o.err == nil {
		o.t.Fatalf("spectest: expected an error, got nil (events: %#v)", o.events)
	}
	if check == nil {
		o.t.Fatal("spectest: ThenErrorCheck func cannot be nil")
	}
	check(o.err)
	return o
}
