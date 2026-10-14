package outbox

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore() (*MemoryStore, *fakeClock) {
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := NewMemoryStore()
	s.now = c.now
	return s, c
}

func enqueue(t *testing.T, s *MemoryStore, key string, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rec := Record{Topic: "t", Payload: []byte(strconv.Itoa(i))}
		if key != "" {
			rec.Key = []byte(key)
		}
		id, err := s.Enqueue(context.Background(), rec)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func idsOf(recs []Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.ID
	}
	return out
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestClaimReturnsOldestFirstUpToLimit(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 5)

	got, err := s.Claim(ctx, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !equalIDs(idsOf(got), ids[:3]) {
		t.Fatalf("got %v, want %v", idsOf(got), ids[:3])
	}

	// The remaining two are still claimable; the first three are leased.
	got, _ = s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), ids[3:]) {
		t.Fatalf("second claim got %v, want %v", idsOf(got), ids[3:])
	}
}

func TestClaimDoesNotReturnPublishedRecords(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "k", 2)

	_ = s.MarkPublished(ctx, ids[0])
	got, _ := s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), ids[1:]) {
		t.Fatalf("got %v, want %v", idsOf(got), ids[1:])
	}
	if s.Pending() != 1 {
		t.Fatalf("pending = %d, want 1", s.Pending())
	}
}

func TestClaimBlocksKeyWhileOlderRecordIsLeased(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "k", 1)
	if got, _ := s.Claim(ctx, 1, time.Minute); len(got) != 1 {
		t.Fatalf("setup claim got %d records", len(got))
	}

	// A newer record with the same key arrives while the first is leased.
	newer := enqueue(t, s, "k", 1)
	if got, _ := s.Claim(ctx, 10, time.Minute); len(got) != 0 {
		t.Fatalf("claimed %v while %s is leased", idsOf(got), ids[0])
	}

	// Other keys and unkeyed records are unaffected.
	other := enqueue(t, s, "other", 1)
	unkeyed := enqueue(t, s, "", 1)
	got, _ := s.Claim(ctx, 10, time.Minute)
	if want := append(other, unkeyed...); !equalIDs(idsOf(got), want) {
		t.Fatalf("got %v, want %v", idsOf(got), want)
	}

	_ = s.MarkPublished(ctx, ids...)
	got, _ = s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), newer) {
		t.Fatalf("after publish got %v, want %v", idsOf(got), newer)
	}
}

func TestUnkeyedRecordsDoNotBlockEachOther(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 2)

	_, _ = s.Claim(ctx, 1, time.Minute) // leases ids[0]
	got, _ := s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), ids[1:]) {
		t.Fatalf("got %v, want %v", idsOf(got), ids[1:])
	}
}

func TestExpiredLeaseMakesRecordClaimableAgain(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	ids := enqueue(t, s, "k", 1)

	_, _ = s.Claim(ctx, 10, time.Second)
	if got, _ := s.Claim(ctx, 10, time.Second); len(got) != 0 {
		t.Fatal("record claimed twice within its lease")
	}

	clock.advance(2 * time.Second) // the relay "crashed"
	got, _ := s.Claim(ctx, 10, time.Second)
	if !equalIDs(idsOf(got), ids) {
		t.Fatalf("got %v, want %v after lease expiry", idsOf(got), ids)
	}
}

func TestMarkFailedCountsAttemptAndBacksOff(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	ids := enqueue(t, s, "k", 2)

	_, _ = s.Claim(ctx, 1, time.Minute)
	if err := s.MarkFailed(ctx, ids[0], errors.New("boom"), 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// While backing off, neither the failed record nor its key successor is claimable.
	clock.advance(9 * time.Second)
	if got, _ := s.Claim(ctx, 10, time.Minute); len(got) != 0 {
		t.Fatalf("claimed %v during backoff", idsOf(got))
	}

	clock.advance(2 * time.Second)
	got, _ := s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), ids) {
		t.Fatalf("got %v, want %v", idsOf(got), ids)
	}
	if got[0].Attempts != 1 || got[1].Attempts != 0 {
		t.Fatalf("attempts = %d,%d, want 1,0", got[0].Attempts, got[1].Attempts)
	}
}

func TestReleaseDropsLeaseWithoutCountingAttempt(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "k", 1)

	_, _ = s.Claim(ctx, 10, time.Minute)
	_ = s.Release(ctx, ids...)
	got, _ := s.Claim(ctx, 10, time.Minute)
	if len(got) != 1 || got[0].Attempts != 0 {
		t.Fatalf("got %+v, want the record again with 0 attempts", got)
	}
}

func TestReleaseDoesNotResurrectPublishedRecord(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 1)

	_ = s.MarkPublished(ctx, ids...)
	_ = s.Release(ctx, ids...)
	if got, _ := s.Claim(ctx, 10, time.Minute); len(got) != 0 {
		t.Fatalf("published record claimed again: %v", idsOf(got))
	}
}

func TestClaimReturnsCopiesOfHeaders(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()
	_, _ = s.Enqueue(ctx, Record{Headers: map[string]string{"a": "1"}})

	got, _ := s.Claim(ctx, 1, time.Second)
	got[0].Headers["a"] = "mutated"
	_ = s.Release(ctx, got[0].ID)

	again, _ := s.Claim(ctx, 1, time.Second)
	if again[0].Headers["a"] != "1" {
		t.Fatalf("store state leaked through returned headers: %v", again[0].Headers)
	}
}
