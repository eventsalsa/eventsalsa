package persistence

import (
	"errors"
	"io"
	"strings"
	"testing"

	"eventsalsa.dev/store"
	v1 "eventsalsa.dev/store/examples/eventmap-codegen/domain/user/events/v1"
)

func TestEventMap_UnknownEventsTypedNilAndInterfaceMismatch(t *testing.T) {
	t.Parallel()

	if _, err := EventTypeOf(struct{}{}); !errors.Is(err, store.ErrUnknownEvent) {
		t.Fatalf("EventTypeOf unknown err = %v, want store.ErrUnknownEvent", err)
	}
	if _, err := FromESEvent(store.PersistedEvent{EventType: "Missing", EventVersion: 1}); !errors.Is(
		err, store.ErrUnknownEvent,
	) {
		t.Fatalf("FromESEvent unknown type err = %v, want store.ErrUnknownEvent", err)
	}
	if _, err := FromESEvent(store.PersistedEvent{EventType: "UserRegistered", EventVersion: 99}); !errors.Is(
		err, store.ErrUnknownEvent,
	) {
		t.Fatalf("FromESEvent unknown version err = %v, want store.ErrUnknownEvent", err)
	}

	var nilPtr *v1.UserDeleted
	if _, _, _, err := MarshalEvent(nilPtr); err == nil {
		t.Fatal("expected error marshaling typed nil pointer")
	}
	if _, err := ToESEvents("User", "u-1", []*v1.UserDeleted{nilPtr}); err == nil {
		t.Fatal("expected error in ToESEvents for typed nil pointer")
	}

	pe := store.PersistedEvent{EventType: "UserDeleted", EventVersion: 1, Payload: []byte(`{}`)}
	_, asErr := FromESEventAs[io.Reader](pe)
	if asErr == nil || !strings.Contains(asErr.Error(), "io.Reader") {
		t.Fatalf("FromESEventAs[io.Reader] err = %v, want error naming io.Reader", asErr)
	}
}
