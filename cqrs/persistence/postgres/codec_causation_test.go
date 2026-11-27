package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/persistence/postgres"
	"eventsalsa.dev/store"
)

func TestFuncAndEventMapCodec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var emptyCodec postgres.FuncEventCodec[any]
	if _, err := emptyCodec.Encode(ctx, "Order", "1", orderAdded{Delta: 1}); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("empty Encode err = %v, want ErrNilCodec", err)
	}
	if _, err := emptyCodec.Decode(ctx, store.PersistedEvent{}); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("empty Decode err = %v, want ErrNilCodec", err)
	}

	encodeOnly := postgres.FuncEventCodec[any]{
		EncodeFunc: jsonAccountCodec().Encode,
	}
	if _, err := encodeOnly.Decode(ctx, store.PersistedEvent{}); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("encodeOnly Decode err = %v, want ErrNilCodec", err)
	}
	decodeOnly := postgres.FuncEventCodec[any]{
		DecodeFunc: jsonAccountCodec().Decode,
	}
	if _, err := decodeOnly.Encode(ctx, "Order", "1", orderAdded{Delta: 1}); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("decodeOnly Encode err = %v, want ErrNilCodec", err)
	}

	if _, err := postgres.NewEventMapCodec[any](nil, func(store.PersistedEvent) (any, error) { return nil, nil }); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("NewEventMapCodec(nil marshal) err = %v, want ErrNilCodec", err)
	}
	if _, err := postgres.NewEventMapCodec[any](func(any) (string, int, []byte, error) { return "", 0, nil, nil }, nil); !errors.Is(err, postgres.ErrNilCodec) {
		t.Fatalf("NewEventMapCodec(nil unmarshal) err = %v, want ErrNilCodec", err)
	}

	marshalErr := errors.New("marshal boom")
	unmarshalErr := errors.New("unmarshal boom")
	codec, err := postgres.NewEventMapCodec(
		func(e any) (string, int, []byte, error) {
			ev, ok := e.(orderAdded)
			if !ok {
				return "", 0, nil, marshalErr
			}
			b, mErr := json.Marshal(ev)
			return "OrderAdded", 2, b, mErr
		},
		func(pe store.PersistedEvent) (any, error) {
			var ev orderAdded
			if uErr := json.Unmarshal(pe.Payload, &ev); uErr != nil {
				return nil, unmarshalErr
			}
			return ev, nil
		},
	)
	if err != nil {
		t.Fatalf("NewEventMapCodec: %v", err)
	}

	encoded, err := codec.Encode(ctx, "Order", "ord-1", orderAdded{Delta: 42})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encoded.StreamType != "Order" || encoded.StreamID != "ord-1" ||
		encoded.EventType != "OrderAdded" || encoded.EventVersion != 2 {
		t.Fatalf("unexpected encoded metadata: %+v", encoded)
	}
	if _, err := codec.Encode(ctx, "Order", "ord-1", "bad-type"); !errors.Is(err, marshalErr) {
		t.Fatalf("expected marshalErr, got: %v", err)
	}

	decoded, err := codec.Decode(ctx, store.PersistedEvent{Payload: encoded.Payload})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if add, ok := decoded.(orderAdded); !ok || add.Delta != 42 {
		t.Fatalf("decoded = %#v, want orderAdded{Delta: 42}", decoded)
	}
	if _, err := codec.Decode(ctx, store.PersistedEvent{Payload: []byte(`{invalid`)}); !errors.Is(err, unmarshalErr) {
		t.Fatalf("expected unmarshalErr, got: %v", err)
	}

	// Concrete generic event type E != any works identically.
	typedCodec, err := postgres.NewEventMapCodec(
		func(e any) (string, int, []byte, error) {
			b, mErr := json.Marshal(e)
			return "OrderAdded", 1, b, mErr
		},
		func(pe store.PersistedEvent) (orderAdded, error) {
			var ev orderAdded
			uErr := json.Unmarshal(pe.Payload, &ev)
			return ev, uErr
		},
	)
	if err != nil {
		t.Fatalf("NewEventMapCodec[orderAdded]: %v", err)
	}
	encTyped, err := typedCodec.Encode(ctx, "Order", "ord-typed", orderAdded{Delta: 99})
	if err != nil {
		t.Fatalf("typedCodec.Encode: %v", err)
	}
	decTyped, err := typedCodec.Decode(ctx, store.PersistedEvent{Payload: encTyped.Payload})
	if err != nil || decTyped.Delta != 99 {
		t.Fatalf("typedCodec.Decode = (%+v, %v), want Delta=99", decTyped, err)
	}
}

func TestContextWithEventCausation(t *testing.T) {
	t.Parallel()

	eventID := uuid.New()

	// Normal persisted event sets TraceID from pe and CausationID = pe.EventID while clearing CommandID.
	parent := cqrs.WithObservability(context.Background(), cqrs.Observability{
		TraceID:     "outer-trace",
		CommandID:   "stale-cmd",
		CausationID: "stale-cause",
	})
	child := postgres.ContextWithEventCausation(parent, store.PersistedEvent{
		EventID:     eventID,
		TraceID:     store.NullString{String: "ev-trace", Valid: true},
		CausationID: store.NullString{String: "upstream-cause", Valid: true},
	})
	obs, ok := cqrs.ObservabilityFromContext(child)
	if !ok || obs.TraceID != "ev-trace" || obs.CausationID != eventID.String() || obs.CommandID != "" {
		t.Fatalf("unexpected child obs = %+v", obs)
	}

	// command.ObservabilityMiddleware promotes pe.EventID directly onto downstream CausationID.
	var seen cqrs.Observability
	bus, err := command.NewBus(command.WithMiddleware(command.ObservabilityMiddleware(
		command.WithIDGenerator(func() string { return "downstream-cmd-id" }),
	)))
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}
	command.MustRegister(bus, command.Func[string](func(ctx context.Context, _ string) error {
		seen, _ = cqrs.ObservabilityFromContext(ctx)
		return nil
	}))
	if dispatchErr := bus.Dispatch(child, "go"); dispatchErr != nil {
		t.Fatalf("Dispatch: %v", dispatchErr)
	}
	if seen.TraceID != "ev-trace" || seen.CausationID != eventID.String() || seen.CommandID != "downstream-cmd-id" {
		t.Fatalf("downstream obs = %+v, want trace=ev-trace causation=%s cmd=downstream-cmd-id", seen, eventID.String())
	}

	// Missing EventID and empty/invalid pe.TraceID preserve outer trace and fall back to pe.CausationID.
	fallback := postgres.ContextWithEventCausation(
		cqrs.WithTraceID(context.Background(), "kept-trace"),
		store.PersistedEvent{
			TraceID:     store.NullString{String: "", Valid: true},
			CausationID: store.NullString{String: "parent-ev", Valid: true},
		},
	)
	fbObs, _ := cqrs.ObservabilityFromContext(fallback)
	if fbObs.TraceID != "kept-trace" || fbObs.CausationID != "parent-ev" {
		t.Fatalf("fallback obs = %+v, want trace=kept-trace causation=parent-ev", fbObs)
	}

	// Nil EventID + absent pe.CausationID clears any stale outer CommandID/CausationID cleanly.
	cleared := postgres.ContextWithEventCausation(parent, store.PersistedEvent{})
	clrObs, _ := cqrs.ObservabilityFromContext(cleared)
	if clrObs.TraceID != "outer-trace" || clrObs.CommandID != "" || clrObs.CausationID != "" {
		t.Fatalf("cleared obs = %+v, want TraceID=outer-trace and empty CommandID/CausationID", clrObs)
	}

	nilCtxObs, _ := cqrs.ObservabilityFromContext(postgres.ContextWithEventCausation(nil, store.PersistedEvent{EventID: eventID})) //nolint:staticcheck // SA1012: testing nil ctx normalization
	if nilCtxObs.CausationID != eventID.String() {
		t.Fatalf("nil ctx obs = %+v, want causation=%s", nilCtxObs, eventID.String())
	}
}
