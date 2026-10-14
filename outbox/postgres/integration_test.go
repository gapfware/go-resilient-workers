//go:build integration

package postgres_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/gapfware/go-resilient-workers/outbox"
	"github.com/gapfware/go-resilient-workers/outbox/outboxtest"
	"github.com/gapfware/go-resilient-workers/outbox/postgres"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(20)
	t.Cleanup(func() { db.Close() })
	return db
}

// newStore creates a store on a unique table so tests never see each other's rows.
func newStore(t *testing.T) (*postgres.Store, *sql.DB) {
	t.Helper()
	db := openDB(t)
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	table := "outbox_" + hex.EncodeToString(b)
	s, err := postgres.New(db, postgres.WithTable(table))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE " + table) })
	return s, db
}

func TestConformance(t *testing.T) {
	outboxtest.Run(t, func(t *testing.T) (outbox.Store, outboxtest.EnqueueFunc) {
		s, db := newStore(t)
		return s, func(ctx context.Context, rec outbox.Record) (string, error) {
			return s.Enqueue(ctx, db, rec)
		}
	})
}

func TestEnqueueInRolledBackTxLeavesNothing(t *testing.T) {
	ctx := context.Background()
	s, db := newStore(t)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(ctx, tx, outbox.Record{Topic: "t", Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if recs, err := s.Claim(ctx, 10, time.Minute); err != nil || len(recs) != 0 {
		t.Fatalf("claim after rollback = %v, %v", recs, err)
	}

	tx, _ = db.BeginTx(ctx, nil)
	if _, err := s.Enqueue(ctx, tx, outbox.Record{Topic: "t"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if recs, _ := s.Claim(ctx, 10, time.Minute); len(recs) != 1 {
		t.Fatalf("claim after commit = %d records, want 1", len(recs))
	}
}

func TestPrune(t *testing.T) {
	ctx := context.Background()
	s, db := newStore(t)
	old, _ := s.Enqueue(ctx, db, outbox.Record{Topic: "t"})
	fresh, _ := s.Enqueue(ctx, db, outbox.Record{Topic: "t"})
	pending, _ := s.Enqueue(ctx, db, outbox.Record{Topic: "t"})
	if err := s.MarkPublished(ctx, old); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := s.MarkPublished(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	n, err := s.Prune(ctx, 25*time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("pruned %d, %v; want exactly the old published record", n, err)
	}
	// The pending record must survive and still be claimable.
	recs, _ := s.Claim(ctx, 10, time.Minute)
	if len(recs) != 1 || recs[0].ID != pending {
		t.Fatalf("claim after prune = %v, want only %s", recs, pending)
	}
}
