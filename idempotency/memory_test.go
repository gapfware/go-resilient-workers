package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()

	if err := s.Acquire(ctx, "k", time.Minute); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := s.Acquire(ctx, "k", time.Minute); !errors.Is(err, ErrInProgress) {
		t.Fatalf("second acquire while leased: got %v, want ErrInProgress", err)
	}

	if err := s.Complete(ctx, "k", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.Acquire(ctx, "k", time.Minute); !errors.Is(err, ErrAlreadyProcessed) {
		t.Fatalf("acquire after complete: got %v, want ErrAlreadyProcessed", err)
	}

	clock.advance(time.Hour)
	if err := s.Acquire(ctx, "k", time.Minute); err != nil {
		t.Fatalf("acquire after TTL expiry: %v", err)
	}
}

func TestReleaseAllowsRetry(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()

	_ = s.Acquire(ctx, "k", time.Minute)
	_ = s.Release(ctx, "k")
	if err := s.Acquire(ctx, "k", time.Minute); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestReleaseDoesNotForgetCompletedKey(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore()

	_ = s.Acquire(ctx, "k", time.Minute)
	_ = s.Complete(ctx, "k", time.Hour)
	_ = s.Release(ctx, "k")
	if err := s.Acquire(ctx, "k", time.Minute); !errors.Is(err, ErrAlreadyProcessed) {
		t.Fatalf("got %v, want ErrAlreadyProcessed", err)
	}
}

func TestExpiredLeaseCanBeTakenOver(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()

	_ = s.Acquire(ctx, "k", time.Second)
	clock.advance(2 * time.Second) // the original worker "crashed"
	if err := s.Acquire(ctx, "k", time.Second); err != nil {
		t.Fatalf("takeover after lease expiry: %v", err)
	}
}

func TestConcurrentAcquireHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.Acquire(ctx, "shared", time.Minute) == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("got %d winners, want exactly 1", wins.Load())
	}
}

func TestSweep(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	_ = s.Complete(ctx, "a", time.Second)
	_ = s.Complete(ctx, "b", time.Hour)
	clock.advance(time.Minute)
	if n := s.Sweep(); n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
}
