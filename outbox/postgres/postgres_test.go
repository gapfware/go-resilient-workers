package postgres_test

import (
	"errors"
	"testing"

	"github.com/gapfware/go-resilient-workers/outbox/postgres"
)

func TestNewValidatesTableName(t *testing.T) {
	tests := []struct {
		table string
		ok    bool
	}{
		{"outbox", true},
		{"app.outbox_events", true},
		{"_x1", true},
		{"", false},
		{"1outbox", false},
		{"outbox; DROP TABLE users", false},
		{`"outbox"`, false},
		{"a.b.c", false},
	}
	for _, tc := range tests {
		_, err := postgres.New(nil, postgres.WithTable(tc.table))
		if tc.ok && err != nil {
			t.Errorf("%q: unexpected error %v", tc.table, err)
		}
		if !tc.ok && !errors.Is(err, postgres.ErrInvalidTable) {
			t.Errorf("%q: err = %v, want ErrInvalidTable", tc.table, err)
		}
	}
}
