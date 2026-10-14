package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gapfware/go-resilient-workers/retry"
)

// recorder is a Publisher that remembers what it sent, in order, and fails on demand.
type recorder struct {
	mu   sync.Mutex
	sent []string
	// fail decides, per record, whether Publish returns an error.
	fail func(Record) error
	// after runs once Publish succeeded, e.g. to cancel a context mid-batch.
	after func(Record)
}

func (p *recorder) Publish(_ context.Context, rec Record) error {
	if p.fail != nil {
		if err := p.fail(rec); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.sent = append(p.sent, rec.ID)
	p.mu.Unlock()
	if p.after != nil {
		p.after(rec)
	}
	return nil
}

func (p *recorder) published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

func TestFlushPublishesAllInInsertionOrder(t *testing.T) {
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 5)
	pub := &recorder{}

	n, err := NewRelay(s, pub).Flush(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("Flush = %d, %v; want 5, nil", n, err)
	}
	if !equalIDs(pub.published(), ids) {
		t.Fatalf("published %v, want %v", pub.published(), ids)
	}
	if s.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", s.Pending())
	}
}

func TestFlushOnEmptyStoreIsNoop(t *testing.T) {
	s, _ := newTestStore()
	if n, err := NewRelay(s, &recorder{}).Flush(context.Background()); n != 0 || err != nil {
		t.Fatalf("Flush = %d, %v; want 0, nil", n, err)
	}
}

func TestFailureBlocksKeyUntilRetryDelayAndKeepsOrder(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	k := enqueue(t, s, "K", 3)
	other := enqueue(t, s, "L", 1)

	failK1 := true
	pub := &recorder{fail: func(r Record) error {
		if r.ID == k[0] && failK1 {
			return errors.New("broker down")
		}
		return nil
	}}
	relay := NewRelay(s, pub, WithRelayRetryPolicy(retry.Constant(10*time.Second, 0)))

	if _, err := relay.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Only the other key made it; K2 and K3 were released, not published.
	if !equalIDs(pub.published(), other) {
		t.Fatalf("published %v, want %v", pub.published(), other)
	}

	// Before the retry delay nothing of K is claimable, so order cannot be violated.
	clock.advance(5 * time.Second)
	if n, _ := relay.Flush(ctx); n != 0 {
		t.Fatalf("claimed %d records of K during backoff", n)
	}

	failK1 = false
	clock.advance(6 * time.Second)
	if _, err := relay.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if want := append(append([]string(nil), other...), k...); !equalIDs(pub.published(), want) {
		t.Fatalf("published %v, want %v", pub.published(), want)
	}
	if s.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", s.Pending())
	}
}

func TestReleasedRecordsKeepTheirAttempts(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	k := enqueue(t, s, "K", 2)

	pub := &recorder{fail: func(r Record) error {
		if r.ID == k[0] {
			return errors.New("boom")
		}
		return nil
	}}
	relay := NewRelay(s, pub, WithRelayRetryPolicy(retry.Constant(time.Second, 0)))
	_, _ = relay.Flush(ctx)

	clock.advance(2 * time.Second)
	got, _ := s.Claim(ctx, 10, time.Minute)
	if !equalIDs(idsOf(got), k) || got[0].Attempts != 1 || got[1].Attempts != 0 {
		t.Fatalf("got %+v, want K1 with 1 attempt and K2 with 0", got)
	}
}

func TestUnkeyedRecordsAreNeverBlockedByFailures(t *testing.T) {
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 3)
	pub := &recorder{fail: func(r Record) error {
		if r.ID == ids[0] {
			return errors.New("boom")
		}
		return nil
	}}

	_, _ = NewRelay(s, pub).Flush(context.Background())
	if !equalIDs(pub.published(), ids[1:]) {
		t.Fatalf("published %v, want %v", pub.published(), ids[1:])
	}
}

func TestPublishErrorCarriesPolicyDelayAndCountsAttempt(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	ids := enqueue(t, s, "", 1)
	cause := errors.New("broker down")

	var got []error
	relay := NewRelay(s, &recorder{fail: func(Record) error { return cause }},
		WithRelayRetryPolicy(retry.Constant(7*time.Second, 0)),
		WithErrorHandler(func(err error) { got = append(got, err) }),
	)
	if _, err := relay.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	var pe *PublishError
	if len(got) != 1 || !errors.As(got[0], &pe) {
		t.Fatalf("handler got %v, want one *PublishError", got)
	}
	if pe.RetryIn != 7*time.Second || pe.Record.ID != ids[0] || !errors.Is(pe, cause) {
		t.Fatalf("PublishError = %+v, want RetryIn 7s, record %s, wrapping cause", pe, ids[0])
	}

	clock.advance(8 * time.Second)
	rec, _ := s.Claim(ctx, 1, time.Minute)
	if len(rec) != 1 || rec[0].Attempts != 1 {
		t.Fatalf("got %+v, want the record with Attempts=1", rec)
	}
}

func TestExhaustedPolicyStillKeepsRecordWithMaxDelay(t *testing.T) {
	ctx := context.Background()
	s, clock := newTestStore()
	enqueue(t, s, "", 1)

	var pe *PublishError
	relay := NewRelay(s, &recorder{fail: func(Record) error { return errors.New("boom") }},
		WithRelayRetryPolicy(retry.Never()),
		WithErrorHandler(func(err error) { errors.As(err, &pe) }),
	)
	_, _ = relay.Flush(ctx)
	if pe == nil || pe.RetryIn != time.Hour {
		t.Fatalf("PublishError = %+v, want RetryIn 1h", pe)
	}
	if s.Pending() != 1 {
		t.Fatal("record must never be discarded")
	}
	clock.advance(time.Hour + time.Second)
	if got, _ := s.Claim(ctx, 1, time.Minute); len(got) != 1 {
		t.Fatal("record should be claimable again after the fallback delay")
	}
}

func TestCancelMidBatchMarksSentAndReleasesTheRest(t *testing.T) {
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 4)

	ctx, cancel := context.WithCancel(context.Background())
	pub := &recorder{after: func(r Record) {
		if r.ID == ids[1] {
			cancel() // shutdown arrives after the second record was sent
		}
	}}

	if _, err := NewRelay(s, pub).Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if !equalIDs(pub.published(), ids[:2]) {
		t.Fatalf("published %v, want %v", pub.published(), ids[:2])
	}

	// The two sent records are marked; the rest were released, so another
	// relay can take them immediately instead of waiting for the lease.
	if s.Pending() != 2 {
		t.Fatalf("pending = %d, want 2", s.Pending())
	}
	got, _ := s.Claim(context.Background(), 10, time.Minute)
	if !equalIDs(idsOf(got), ids[2:]) || got[0].Attempts != 0 {
		t.Fatalf("got %+v, want %v released with 0 attempts", got, ids[2:])
	}
}

func TestPublishInterruptedByShutdownIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s, _ := newTestStore()
	ids := enqueue(t, s, "", 1)

	var handled int
	relay := NewRelay(s, PublisherFunc(func(ctx context.Context, _ Record) error {
		cancel()
		return ctx.Err()
	}), WithErrorHandler(func(error) { handled++ }))

	if _, err := relay.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatalf("shutdown reported as %d publish errors", handled)
	}
	got, _ := s.Claim(context.Background(), 1, time.Minute)
	if !equalIDs(idsOf(got), ids) || got[0].Attempts != 0 {
		t.Fatalf("got %+v, want the record released with 0 attempts", got)
	}
}

func TestRunDrainsBacklogWithoutWaitingForPollInterval(t *testing.T) {
	s, _ := newTestStore()
	enqueue(t, s, "", 35)
	pub := &recorder{}

	// An hour-long interval proves the backlog drains through the "full
	// batch, poll again immediately" path, not through timer ticks.
	relay := NewRelay(s, pub, WithBatchSize(10), WithPollInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for s.Pending() > 0 {
		select {
		case <-deadline:
			t.Fatalf("backlog not drained: %d pending", s.Pending())
		case <-time.After(time.Millisecond):
		}
	}
	if len(pub.published()) != 35 {
		t.Fatalf("published %d, want 35", len(pub.published()))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on cancel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
