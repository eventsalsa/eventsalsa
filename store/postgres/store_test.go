package postgres

import (
	"strings"
	"testing"

	"eventsalsa.dev/store"
)

// Compile-time interface compliance checks.
var (
	_ store.EventStore   = (*Store)(nil)
	_ store.EventReader  = (*Store)(nil)
	_ store.HeadReader   = (*Store)(nil)
	_ store.StreamReader = (*Store)(nil)
)

func TestWithNotifyChannel(t *testing.T) {
	t.Parallel()

	config := NewStoreConfig(WithNotifyChannel("my_events"))

	if config.NotifyChannel != "my_events" {
		t.Errorf("NotifyChannel = %q, want %q", config.NotifyChannel, "my_events")
	}
}

func TestDefaultStoreConfig_NotifyChannelEmpty(t *testing.T) {
	t.Parallel()

	config := DefaultStoreConfig()

	if config.NotifyChannel != "" {
		t.Errorf("NotifyChannel = %q, want empty (disabled by default)", config.NotifyChannel)
	}
}

func TestBuildReadEventsQuery(t *testing.T) {
	t.Parallel()

	query, args := buildReadEventsQuery("events", store.Cursor{TransactionID: 400, GlobalPosition: 41}, 25)
	normalized := strings.Join(strings.Fields(query), " ")

	for _, want := range []string{
		"SELECT global_position, transaction_id, stream_type",
		"WHERE transaction_id < pg_snapshot_xmin(pg_current_snapshot())",
		"AND (transaction_id, global_position) > ($1::xid8, $2::bigint)",
		"ORDER BY transaction_id ASC, global_position ASC",
		"LIMIT $3",
	} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("query %q missing %q", normalized, want)
		}
	}

	if len(args) != 3 {
		t.Fatalf("len(args) = %d, want 3", len(args))
	}
	if args[0] != "400" {
		t.Fatalf("args[0] = %#v, want %q", args[0], "400")
	}
	if args[1] != int64(41) {
		t.Fatalf("args[1] = %#v, want %d", args[1], int64(41))
	}
	if args[2] != 25 {
		t.Fatalf("args[2] = %#v, want %d", args[2], 25)
	}
}

func TestBuildGetHeadQuery(t *testing.T) {
	t.Parallel()

	query := buildGetHeadQuery("ordering.events")
	normalized := strings.Join(strings.Fields(query), " ")

	for _, want := range []string{
		"SELECT transaction_id, global_position",
		"FROM ordering.events",
		"WHERE transaction_id < pg_snapshot_xmin(pg_current_snapshot())",
		"ORDER BY transaction_id DESC, global_position DESC",
		"LIMIT 1",
	} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("query %q missing %q", normalized, want)
		}
	}
}

func TestReadEvents_NonPositiveLimitReturnsNilWithoutQuery(t *testing.T) {
	t.Parallel()

	s := NewStore(DefaultStoreConfig())
	for _, limit := range []int{0, -1, -100} {
		events, err := s.ReadEvents(t.Context(), nil, store.Cursor{}, limit)
		if err != nil {
			t.Fatalf("ReadEvents(limit=%d) err = %v, want nil", limit, err)
		}
		if events != nil {
			t.Fatalf("ReadEvents(limit=%d) events = %v, want nil", limit, events)
		}
	}
}
