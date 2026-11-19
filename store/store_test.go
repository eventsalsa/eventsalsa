package store

import (
	"math"
	"testing"
)

func TestCursor_IsZero(t *testing.T) {
	tests := []struct {
		name   string
		cursor Cursor
		want   bool
	}{
		{name: "zero value", cursor: Cursor{}, want: true},
		{name: "transaction id only", cursor: Cursor{TransactionID: 1}, want: false},
		{name: "global position only", cursor: Cursor{GlobalPosition: 1}, want: false},
		{name: "both set", cursor: Cursor{TransactionID: 42, GlobalPosition: 100}, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.cursor.IsZero(); got != tc.want {
				t.Fatalf("Cursor(%+v).IsZero() = %v, want %v", tc.cursor, got, tc.want)
			}
		})
	}
}

func TestCursor_Before(t *testing.T) {
	tests := []struct {
		name string
		a    Cursor
		b    Cursor
		want bool
	}{
		{
			name: "equal zero cursors",
			a:    Cursor{},
			b:    Cursor{},
			want: false,
		},
		{
			name: "equal non-zero cursors",
			a:    Cursor{TransactionID: 10, GlobalPosition: 5},
			b:    Cursor{TransactionID: 10, GlobalPosition: 5},
			want: false,
		},
		{
			name: "zero precedes legacy position",
			a:    Cursor{},
			b:    Cursor{TransactionID: 0, GlobalPosition: 1},
			want: true,
		},
		{
			name: "same transaction id orders by global position",
			a:    Cursor{TransactionID: 10, GlobalPosition: 4},
			b:    Cursor{TransactionID: 10, GlobalPosition: 5},
			want: true,
		},
		{
			name: "same transaction id higher position does not precede",
			a:    Cursor{TransactionID: 10, GlobalPosition: 6},
			b:    Cursor{TransactionID: 10, GlobalPosition: 5},
			want: false,
		},
		{
			name: "lower transaction id with higher position still precedes",
			a:    Cursor{TransactionID: 9, GlobalPosition: 999},
			b:    Cursor{TransactionID: 10, GlobalPosition: 1},
			want: true,
		},
		{
			name: "higher transaction id with lower position does not precede",
			a:    Cursor{TransactionID: 10, GlobalPosition: 1},
			b:    Cursor{TransactionID: 9, GlobalPosition: 999},
			want: false,
		},
		{
			name: "uint64 high bit ordering",
			a:    Cursor{TransactionID: math.MaxInt64, GlobalPosition: 99},
			b:    Cursor{TransactionID: math.MaxInt64 + 1, GlobalPosition: 1},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Before(tc.b); got != tc.want {
				t.Fatalf("%+v.Before(%+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
