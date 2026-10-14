// Package outboxtest is a conformance suite for outbox.Store implementations.
//
// Every store must honour the same leasing and per-key ordering rules, or the
// Relay's guarantees silently break. Running one suite against MemoryStore and
// the Postgres store keeps the in-memory fake honest.
package outboxtest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gapfware/go-resilient-workers/outbox"
)

// EnqueueFunc inserts a record and returns its ID.
type EnqueueFunc func(ctx context.Context, rec outbox.Record) (string, error)

// Factory returns a fresh, empty store and a way to add records to it. Stores
// have no common enqueue method (Postgres enqueues inside a caller's
// transaction), so the factory supplies it. Use t.Cleanup for teardown.
type Factory func(t *testing.T) (outbox.Store, EnqueueFunc)

// Time scale for lease and backoff tests: real sleeps, because a database
// store uses its own clock and cannot be driven by a fake one.
const (
	shortLease = 150 * time.Millisecond
	wait       = 250 * time.Millisecond
)

// Run executes the conformance suite against stores built by newStore.
func Run(t *testing.T, newStore Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, outbox.Store, EnqueueFunc)
	}{
		{"ClaimOldestFirstUpToLimit", claimOldestFirst},
		{"ClaimSkipsPublished", claimSkipsPublished},
		{"ClaimReturnsRecordFields", claimReturnsFields},
		{"KeyBlockedWhileOlderRecordLeased", keyBlockedWhileLeased},
		{"UnkeyedRecordsDoNotBlockEachOther", unkeyedIndependent},
		{"ExpiredLeaseIsClaimableAgain", expiredLease},
		{"MarkFailedCountsAttemptAndBlocksKey", markFailedBlocksKey},
		{"ReleaseDoesNotCountAttempt", releaseNoAttempt},
		{"ReleaseDoesNotResurrectPublished", releaseKeepsPublished},
		{"ConcurrentClaimersPublishEachRecordOnceInKeyOrder", concurrentClaimers},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, enqueue := newStore(t)
			tc.fn(t, store, enqueue)
		})
	}
}

func add(t *testing.T, enqueue EnqueueFunc, key string, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rec := outbox.Record{Topic: "t", Payload: []byte(strconv.Itoa(i))}
		if key != "" {
			rec.Key = []byte(key)
		}
		id, err := enqueue(context.Background(), rec)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func claim(t *testing.T, s outbox.Store, limit int, lease time.Duration) []string {
	t.Helper()
	recs, err := s.Claim(context.Background(), limit, lease)
	if err != nil {
		t.Fatal(err)
	}
	return idsOf(recs)
}

func idsOf(recs []outbox.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.ID
	}
	return out
}

func mustEqual(t *testing.T, got, want []string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func claimOldestFirst(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "", 5)
	mustEqual(t, claim(t, s, 3, time.Minute), ids[:3])
	mustEqual(t, claim(t, s, 10, time.Minute), ids[3:])
}

func claimSkipsPublished(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "k", 2)
	if err := s.MarkPublished(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, claim(t, s, 10, time.Minute), ids[1:])
}

func claimReturnsFields(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ctx := context.Background()
	id, err := enqueue(ctx, outbox.Record{
		Topic:   "orders",
		Key:     []byte("k1"),
		Payload: []byte("payload"),
		Headers: map[string]string{"a": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := s.Claim(ctx, 1, time.Minute)
	if err != nil || len(recs) != 1 {
		t.Fatalf("claim = %v, %v", recs, err)
	}
	r := recs[0]
	if r.ID != id || r.Topic != "orders" || string(r.Key) != "k1" ||
		string(r.Payload) != "payload" || r.Headers["a"] != "1" || r.Attempts != 0 || r.CreatedAt.IsZero() {
		t.Fatalf("unexpected record %+v", r)
	}
}

func keyBlockedWhileLeased(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	first := add(t, enqueue, "k", 1)
	mustEqual(t, claim(t, s, 1, time.Minute), first)

	newer := add(t, enqueue, "k", 1)
	mustEqual(t, claim(t, s, 10, time.Minute), nil)

	other := add(t, enqueue, "other", 1)
	unkeyed := add(t, enqueue, "", 1)
	mustEqual(t, claim(t, s, 10, time.Minute), append(other, unkeyed...))

	if err := s.MarkPublished(context.Background(), first...); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, claim(t, s, 10, time.Minute), newer)
}

func unkeyedIndependent(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "", 2)
	claim(t, s, 1, time.Minute)
	mustEqual(t, claim(t, s, 10, time.Minute), ids[1:])
}

func expiredLease(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "k", 1)
	claim(t, s, 10, shortLease)
	mustEqual(t, claim(t, s, 10, shortLease), nil)

	time.Sleep(wait) // the relay "crashed"
	mustEqual(t, claim(t, s, 10, shortLease), ids)
}

func markFailedBlocksKey(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "k", 2)
	claim(t, s, 1, time.Minute)
	if err := s.MarkFailed(context.Background(), ids[0], errors.New("boom"), shortLease); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, claim(t, s, 10, time.Minute), nil) // backing off blocks the key

	time.Sleep(wait)
	recs, err := s.Claim(context.Background(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mustEqual(t, idsOf(recs), ids)
	if recs[0].Attempts != 1 || recs[1].Attempts != 0 {
		t.Fatalf("attempts = %d,%d, want 1,0", recs[0].Attempts, recs[1].Attempts)
	}
}

func releaseNoAttempt(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "k", 1)
	claim(t, s, 10, time.Minute)
	if err := s.Release(context.Background(), ids...); err != nil {
		t.Fatal(err)
	}
	recs, _ := s.Claim(context.Background(), 10, time.Minute)
	if len(recs) != 1 || recs[0].Attempts != 0 {
		t.Fatalf("got %+v, want the record again with 0 attempts", recs)
	}
}

func releaseKeepsPublished(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	ids := add(t, enqueue, "", 1)
	ctx := context.Background()
	if err := s.MarkPublished(ctx, ids...); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, ids...); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, claim(t, s, 10, time.Minute), nil)
}

// concurrentClaimers is rule 6 end to end: two relays over 1000 records in 20
// keys publish each record exactly once and each key in insertion order.
func concurrentClaimers(t *testing.T, s outbox.Store, enqueue EnqueueFunc) {
	const total, keys = 1000, 20
	want := make(map[string][]string)
	for i := 0; i < total; i++ {
		k := fmt.Sprintf("key-%d", i%keys)
		want[k] = append(want[k], add(t, enqueue, k, 1)...)
	}

	ctx := context.Background()
	var (
		mu        sync.Mutex
		published = make(map[string][]string) // key -> IDs in publish order
		count     int
		wg        sync.WaitGroup
		errc      = make(chan error, 2)
	)
	worker := func() {
		defer wg.Done()
		for {
			mu.Lock()
			done := count >= total
			mu.Unlock()
			if done {
				return
			}
			recs, err := s.Claim(ctx, 25, time.Minute)
			if err != nil {
				errc <- err
				return
			}
			for _, r := range recs {
				// Record before MarkPublished: the lease is what orders keys.
				mu.Lock()
				published[string(r.Key)] = append(published[string(r.Key)], r.ID)
				count++
				mu.Unlock()
				if err := s.MarkPublished(ctx, r.ID); err != nil {
					errc <- err
					return
				}
			}
		}
	}
	wg.Add(2)
	go worker()
	go worker()
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}

	for k, w := range want {
		if fmt.Sprint(published[k]) != fmt.Sprint(w) {
			t.Fatalf("key %s published out of order or not exactly once:\n got %v\nwant %v", k, published[k], w)
		}
	}
	if count != total {
		t.Fatalf("published %d records, want %d", count, total)
	}
}
