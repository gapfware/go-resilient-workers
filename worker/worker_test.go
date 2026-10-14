package worker_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gapfware/go-resilient-workers/idempotency"
	"github.com/gapfware/go-resilient-workers/memory"
	"github.com/gapfware/go-resilient-workers/retry"
	"github.com/gapfware/go-resilient-workers/worker"
)

var fast = retry.Constant(time.Millisecond, 3)

// runToCompletion publishes msgs, closes the queue and runs the worker until
// it drains it.
func runToCompletion(t *testing.T, h worker.Handler, msgs []*worker.Message, opts ...worker.Option) (*memory.Queue, *worker.Stats) {
	t.Helper()
	q := memory.NewQueue("test", len(msgs))
	for _, m := range msgs {
		if err := q.Publish(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	q.Close()

	stats := &worker.Stats{}
	opts = append([]worker.Option{worker.WithObserver(stats)}, opts...)
	w := worker.New(q, h, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	return q, stats
}

func msg(key string) *worker.Message { return &worker.Message{Key: []byte(key), Value: []byte(key)} }

func TestSuccessIsCommitted(t *testing.T) {
	var calls atomic.Int32
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { calls.Add(1); return nil })

	q, stats := runToCompletion(t, h, []*worker.Message{msg("a"), msg("b"), msg("c")}, worker.WithConcurrency(2))

	if calls.Load() != 3 || q.Committed() != 3 {
		t.Fatalf("calls=%d committed=%d, want 3/3", calls.Load(), q.Committed())
	}
	if s := stats.Snapshot(); s.Succeeded != 3 || s.Retried != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestTransientErrorIsRetriedUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	h := worker.HandlerFunc(func(_ context.Context, m *worker.Message) error {
		calls.Add(1)
		if m.Attempt < 3 {
			return errors.New("provider unavailable")
		}
		return nil
	})

	q, stats := runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithRetryPolicy(retry.Constant(time.Millisecond, 5)))

	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want 3", calls.Load())
	}
	if s := stats.Snapshot(); s.Succeeded != 1 || s.Retried != 2 || q.Committed() != 1 {
		t.Fatalf("stats %+v committed=%d", s, q.Committed())
	}
}

func TestPermanentErrorGoesStraightToDeadLetter(t *testing.T) {
	var calls atomic.Int32
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error {
		calls.Add(1)
		return worker.Permanent(errors.New("unknown plan"))
	})
	dlq := &memory.DeadLetterQueue{}

	q, _ := runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithRetryPolicy(fast), worker.WithDeadLetter(dlq))

	if calls.Load() != 1 {
		t.Fatalf("permanent errors must not be retried: calls=%d", calls.Load())
	}
	entries := dlq.Entries()
	if len(entries) != 1 || !worker.IsPermanent(entries[0].Cause) {
		t.Fatalf("dlq entries %+v", entries)
	}
	if q.Committed() != 1 {
		t.Fatalf("dead-lettered message must be committed")
	}
}

func TestExhaustedRetriesGoToDeadLetter(t *testing.T) {
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { return errors.New("boom") })
	dlq := &memory.DeadLetterQueue{}

	q, stats := runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithRetryPolicy(fast), worker.WithDeadLetter(dlq))

	entries := dlq.Entries()
	if len(entries) != 1 || !errors.Is(entries[0].Cause, worker.ErrRetriesExhausted) {
		t.Fatalf("want 1 entry wrapping ErrRetriesExhausted, got %+v", entries)
	}
	if entries[0].Message.Attempt != 3 {
		t.Fatalf("attempt=%d, want 3", entries[0].Message.Attempt)
	}
	if s := stats.Snapshot(); s.DeadLettered != 1 || s.Retried != 2 || q.Committed() != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestWithoutDeadLetterFailuresAreDropped(t *testing.T) {
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { return worker.Permanent(errors.New("bad")) })
	q, stats := runToCompletion(t, h, []*worker.Message{msg("a")})
	if stats.Snapshot().Dropped != 1 || q.Committed() != 1 {
		t.Fatalf("stats %+v", stats.Snapshot())
	}
}

func TestIdempotencySkipsDuplicates(t *testing.T) {
	var calls atomic.Int32
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { calls.Add(1); return nil })

	dup := func() *worker.Message {
		return &worker.Message{Key: []byte("acc-1"), Headers: map[string]string{"Idempotency-Key": "activate-acc-1"}}
	}
	q, stats := runToCompletion(t, h, []*worker.Message{dup(), dup(), dup()},
		worker.WithIdempotency(idempotency.NewMemoryStore(), worker.KeyFromHeader("Idempotency-Key"), time.Minute, time.Hour))

	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", calls.Load())
	}
	if s := stats.Snapshot(); s.Duplicates != 2 || q.Committed() != 3 {
		t.Fatalf("stats %+v committed=%d", s, q.Committed())
	}
}

func TestFailedAttemptReleasesIdempotencyKey(t *testing.T) {
	h := worker.HandlerFunc(func(_ context.Context, m *worker.Message) error {
		if m.Attempt == 1 {
			return errors.New("transient")
		}
		return nil
	})
	m := &worker.Message{Headers: map[string]string{"Idempotency-Key": "k"}}
	_, stats := runToCompletion(t, h, []*worker.Message{m},
		worker.WithRetryPolicy(fast),
		worker.WithIdempotency(idempotency.NewMemoryStore(), worker.KeyFromHeader("Idempotency-Key"), time.Minute, time.Hour))

	if s := stats.Snapshot(); s.Succeeded != 1 || s.Duplicates != 0 {
		t.Fatalf("retry after failure must re-acquire the key: %+v", s)
	}
}

func TestPanicIsRecoveredAndRetried(t *testing.T) {
	h := worker.HandlerFunc(func(_ context.Context, m *worker.Message) error {
		if m.Attempt == 1 {
			panic("nil map")
		}
		return nil
	})
	var sawPanic atomic.Bool
	obs := worker.ObserverFunc(func(e worker.Event) {
		var p *worker.PanicError
		if e.Kind == worker.EventRetrying && errors.As(e.Err, &p) {
			sawPanic.Store(true)
		}
	})
	_, stats := runToCompletion(t, h, []*worker.Message{msg("a")}, worker.WithRetryPolicy(fast), worker.WithObserver(obs))

	if !sawPanic.Load() || stats.Snapshot().Succeeded != 1 {
		t.Fatalf("panic should be retried and then succeed: %+v", stats.Snapshot())
	}
}

func TestRetryAfterOverridesPolicyDelay(t *testing.T) {
	h := worker.HandlerFunc(func(_ context.Context, m *worker.Message) error {
		if m.Attempt == 1 {
			return worker.RetryAfter(errors.New("rate limited"), 7*time.Millisecond)
		}
		return nil
	})
	var delay atomic.Int64
	obs := worker.ObserverFunc(func(e worker.Event) {
		if e.Kind == worker.EventRetrying {
			delay.Store(int64(e.Delay))
		}
	})
	runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithRetryPolicy(retry.Constant(time.Hour, 5)), worker.WithObserver(obs))

	if time.Duration(delay.Load()) != 7*time.Millisecond {
		t.Fatalf("delay=%v, want 7ms", time.Duration(delay.Load()))
	}
}

func TestHandlerTimeoutIsRetryable(t *testing.T) {
	h := worker.HandlerFunc(func(ctx context.Context, m *worker.Message) error {
		if m.Attempt == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	_, stats := runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithRetryPolicy(fast), worker.WithHandlerTimeout(10*time.Millisecond))
	if s := stats.Snapshot(); s.Retried != 1 || s.Succeeded != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestDeadLetterOutageIsRetried(t *testing.T) {
	var sends atomic.Int32
	dlq := worker.DeadLetterFunc(func(context.Context, *worker.Message, error) error {
		if sends.Add(1) < 3 {
			return errors.New("dlq topic unavailable")
		}
		return nil
	})
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { return worker.Permanent(errors.New("bad")) })

	q, stats := runToCompletion(t, h, []*worker.Message{msg("a")},
		worker.WithDeadLetter(dlq), worker.WithDeadLetterRetryPolicy(retry.Constant(time.Millisecond, 0)))

	if sends.Load() != 3 || q.Committed() != 1 || stats.Snapshot().DeadLettered != 1 {
		t.Fatalf("sends=%d committed=%d stats=%+v", sends.Load(), q.Committed(), stats.Snapshot())
	}
}

func TestShutdownWaitsForInFlightHandler(t *testing.T) {
	q := memory.NewQueue("test", 1)
	_ = q.Publish(context.Background(), msg("a"))

	started := make(chan struct{})
	var finished atomic.Bool
	h := worker.HandlerFunc(func(ctx context.Context, _ *worker.Message) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		finished.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(q, h)
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	<-started
	cancel() // shutdown while the handler is mid-flight

	if err := <-errc; err != nil {
		t.Fatalf("run: %v", err)
	}
	if !finished.Load() || q.Committed() != 1 {
		t.Fatalf("in-flight handler must complete and commit: finished=%v committed=%d", finished.Load(), q.Committed())
	}
}

func TestShutdownAbandonsMessagesWaitingForRetry(t *testing.T) {
	q := memory.NewQueue("test", 1)
	_ = q.Publish(context.Background(), msg("a"))

	failed := make(chan struct{}, 1)
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error {
		select {
		case failed <- struct{}{}:
		default:
		}
		return errors.New("transient")
	})

	ctx, cancel := context.WithCancel(context.Background())
	stats := &worker.Stats{}
	w := worker.New(q, h, worker.WithObserver(stats), worker.WithRetryPolicy(retry.Constant(time.Hour, 0)))
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	<-failed
	start := time.Now()
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("run: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown must not wait for the 1h backoff")
	}
	if q.Committed() != 0 || len(q.Uncommitted()) != 1 || stats.Snapshot().Abandoned != 1 {
		t.Fatalf("message must stay uncommitted for redelivery: committed=%d stats=%+v", q.Committed(), stats.Snapshot())
	}
}

func TestShutdownTimeoutCancelsHandlers(t *testing.T) {
	q := memory.NewQueue("test", 1)
	_ = q.Publish(context.Background(), msg("a"))

	started := make(chan struct{})
	h := worker.HandlerFunc(func(ctx context.Context, _ *worker.Message) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(q, h, worker.WithShutdownTimeout(20*time.Millisecond), worker.WithRetryPolicy(retry.Never()))
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	<-started
	cancel()
	if err := <-errc; !errors.Is(err, worker.ErrShutdownTimeout) {
		t.Fatalf("got %v, want ErrShutdownTimeout", err)
	}
}

func TestKeyOrderingProcessesSameKeySequentially(t *testing.T) {
	const perKey = 50
	keys := []string{"acc-1", "acc-2", "acc-3"}

	var msgs []*worker.Message
	for i := 0; i < perKey; i++ {
		for _, k := range keys {
			msgs = append(msgs, &worker.Message{Key: []byte(k), Value: []byte(strconv.Itoa(i))})
		}
	}

	var mu sync.Mutex
	seen := map[string][]int{}
	active := map[string]int{}
	var overlap atomic.Bool

	h := worker.HandlerFunc(func(_ context.Context, m *worker.Message) error {
		k := string(m.Key)
		mu.Lock()
		active[k]++
		if active[k] > 1 {
			overlap.Store(true)
		}
		mu.Unlock()

		time.Sleep(100 * time.Microsecond)
		n, _ := strconv.Atoi(string(m.Value))

		mu.Lock()
		seen[k] = append(seen[k], n)
		active[k]--
		mu.Unlock()
		return nil
	})

	runToCompletion(t, h, msgs, worker.WithConcurrency(8), worker.WithKeyOrdering())

	if overlap.Load() {
		t.Fatal("two messages with the same key ran concurrently")
	}
	for _, k := range keys {
		for i, n := range seen[k] {
			if n != i {
				t.Fatalf("key %s processed out of order: %v", k, seen[k])
			}
		}
	}
}

func TestMiddlewareOrder(t *testing.T) {
	var trace []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	mw := func(name string) worker.Middleware {
		return func(next worker.Handler) worker.Handler {
			return worker.HandlerFunc(func(ctx context.Context, m *worker.Message) error {
				record(name + ">")
				err := next.Handle(ctx, m)
				record("<" + name)
				return err
			})
		}
	}
	h := worker.HandlerFunc(func(context.Context, *worker.Message) error { record("handler"); return nil })
	runToCompletion(t, h, []*worker.Message{msg("a")}, worker.WithMiddleware(mw("outer"), mw("inner")))

	want := "[outer> inner> handler <inner <outer]"
	if got := fmt.Sprint(trace); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestRunTwiceConcurrentlyFails(t *testing.T) {
	q := memory.NewQueue("test", 0)
	w := worker.New(q, worker.HandlerFunc(func(context.Context, *worker.Message) error { return nil }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	if err := w.Run(ctx); !errors.Is(err, worker.ErrAlreadyRunning) {
		t.Fatalf("got %v, want ErrAlreadyRunning", err)
	}
	cancel()
	<-errc
}
