package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gapfware/go-resilient-workers/idempotency"
	"github.com/gapfware/go-resilient-workers/retry"
)

// Worker pulls messages from a Source and runs them through a Handler.
type Worker struct {
	source  Source
	handler Handler

	concurrency     int
	keyOrdered      bool
	policy          retry.Policy
	dlqPolicy       retry.Policy
	deadLetter      DeadLetter
	idem            *idemConfig
	handlerTimeout  time.Duration
	shutdownTimeout time.Duration
	fetchBackoff    time.Duration
	observer        Observer
	observers       []Observer
	middleware      []Middleware

	running  atomic.Bool
	nextLane atomic.Uint64
}

// errDuplicate signals internally that the idempotency store already saw the
// message.
var errDuplicate = errors.New("worker: duplicate message")

// New builds a Worker. Run starts it.
func New(source Source, handler Handler, opts ...Option) *Worker {
	if source == nil {
		panic("worker: nil source")
	}
	if handler == nil {
		panic("worker: nil handler")
	}
	w := &Worker{
		source:          source,
		handler:         handler,
		concurrency:     1,
		policy:          retry.Default(),
		dlqPolicy:       retry.Exponential{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Jitter: 0.2},
		shutdownTimeout: 30 * time.Second,
		fetchBackoff:    time.Second,
	}
	for _, opt := range opts {
		opt(w)
	}
	w.observer = Observers(w.observers...)
	for i := len(w.middleware) - 1; i >= 0; i-- {
		w.handler = w.middleware[i](w.handler)
	}
	return w
}

// Run fetches and processes messages until ctx is cancelled or the source
// reports ErrSourceClosed, then waits for in-flight messages to finish.
//
// Cancelling ctx stops fetching immediately. Handlers already running keep
// their own context until they finish or the shutdown timeout elapses.
// Messages waiting for a retry are abandoned (left uncommitted) so that
// shutdown is not delayed by long backoffs.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer w.running.Store(false)

	// Handlers run on a context that survives ctx's cancellation, so a
	// shutdown does not interrupt a half-done side effect.
	procCtx, cancelProc := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelProc()

	lanes := make([]chan *Message, w.concurrency)
	var wg sync.WaitGroup
	for i := range lanes {
		lanes[i] = make(chan *Message)
		wg.Add(1)
		go func(in <-chan *Message) {
			defer wg.Done()
			for msg := range in {
				w.process(procCtx, ctx, msg)
			}
		}(lanes[i])
	}

	w.fetchLoop(ctx, lanes)

	for _, l := range lanes {
		close(l)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.shutdownTimeout):
		cancelProc()
		<-done
		return ErrShutdownTimeout
	}
}

func (w *Worker) fetchLoop(ctx context.Context, lanes []chan *Message) {
	for {
		msg, err := w.source.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, ErrSourceClosed) {
				return
			}
			w.observer.Observe(Event{Kind: EventError, Op: "fetch", Err: err})
			if !sleep(ctx, w.fetchBackoff) {
				return
			}
			continue
		}

		w.observer.Observe(Event{Kind: EventReceived, Message: msg})

		select {
		case lanes[w.laneFor(msg, len(lanes))] <- msg:
		case <-ctx.Done():
			// Fetched but never started: leave it uncommitted.
			w.observer.Observe(Event{Kind: EventAbandoned, Message: msg, Err: ctx.Err()})
			return
		}
	}
}

func (w *Worker) laneFor(msg *Message, n int) int {
	if n == 1 {
		return 0
	}
	if w.keyOrdered && len(msg.Key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(msg.Key)
		return int(h.Sum32() % uint32(n))
	}
	return int(w.nextLane.Add(1) % uint64(n))
}

// process drives one message to a terminal state. procCtx is given to the
// handler; stopCtx signals shutdown and interrupts backoff waits.
func (w *Worker) process(procCtx, stopCtx context.Context, msg *Message) {
	start := time.Now()
	var key string
	if w.idem != nil {
		key = w.idem.key(msg)
	}

	for attempt := 1; ; attempt++ {
		msg.Attempt = attempt
		err := w.attempt(procCtx, msg, key)

		switch {
		case err == nil:
			w.commit(procCtx, msg, Event{Kind: EventSucceeded, Message: msg, Attempt: attempt, Duration: time.Since(start)})
			return
		case errors.Is(err, errDuplicate):
			w.commit(procCtx, msg, Event{Kind: EventDuplicate, Message: msg, Attempt: attempt, Duration: time.Since(start)})
			return
		case IsPermanent(err):
			w.fail(procCtx, stopCtx, msg, err, start)
			return
		}

		delay, ok := w.policy.Backoff(attempt)
		if !ok {
			w.fail(procCtx, stopCtx, msg, fmt.Errorf("%w after %d attempts: %w", ErrRetriesExhausted, attempt, err), start)
			return
		}
		if d, has := retryAfterDelay(err); has {
			delay = d
		}

		w.observer.Observe(Event{Kind: EventRetrying, Message: msg, Attempt: attempt, Err: err, Delay: delay})
		if !sleep(stopCtx, delay) {
			w.observer.Observe(Event{Kind: EventAbandoned, Message: msg, Attempt: attempt, Err: err, Duration: time.Since(start)})
			return
		}
	}
}

// attempt runs the handler once, wrapped by the idempotency guard.
func (w *Worker) attempt(ctx context.Context, msg *Message, key string) error {
	if key != "" {
		switch err := w.idem.store.Acquire(ctx, key, w.idem.lease); {
		case errors.Is(err, idempotency.ErrAlreadyProcessed):
			return errDuplicate
		case errors.Is(err, idempotency.ErrInProgress):
			return err // another worker has it; retry later
		case err != nil:
			w.observer.Observe(Event{Kind: EventError, Op: "idempotency", Message: msg, Err: err})
			return err
		}
	}

	err := w.safeHandle(ctx, msg)

	if key != "" {
		if err == nil {
			// The side effect already happened: report a failure to record
			// it, but do not turn the success into a retry.
			if cerr := w.idem.store.Complete(ctx, key, w.idem.ttl); cerr != nil {
				w.observer.Observe(Event{Kind: EventError, Op: "idempotency", Message: msg, Err: cerr})
			}
		} else if rerr := w.idem.store.Release(ctx, key); rerr != nil {
			w.observer.Observe(Event{Kind: EventError, Op: "idempotency", Message: msg, Err: rerr})
		}
	}
	return err
}

func (w *Worker) safeHandle(ctx context.Context, msg *Message) (err error) {
	if w.handlerTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.handlerTimeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return w.handler.Handle(ctx, msg)
}

// fail routes a message that will not be retried any more.
func (w *Worker) fail(procCtx, stopCtx context.Context, msg *Message, cause error, start time.Time) {
	if w.deadLetter == nil {
		w.commit(procCtx, msg, Event{Kind: EventDropped, Message: msg, Attempt: msg.Attempt, Err: cause, Duration: time.Since(start)})
		return
	}

	for n := 1; ; n++ {
		err := w.deadLetter.Send(procCtx, msg, cause)
		if err == nil {
			w.commit(procCtx, msg, Event{Kind: EventDeadLettered, Message: msg, Attempt: msg.Attempt, Err: cause, Duration: time.Since(start)})
			return
		}
		w.observer.Observe(Event{Kind: EventError, Op: "dead_letter", Message: msg, Attempt: msg.Attempt, Err: err})

		delay, ok := w.dlqPolicy.Backoff(n)
		if !ok || !sleep(stopCtx, delay) {
			// Never commit what the DLQ did not accept: let the source
			// redeliver it.
			w.observer.Observe(Event{Kind: EventAbandoned, Message: msg, Attempt: msg.Attempt, Err: err, Duration: time.Since(start)})
			return
		}
	}
}

func (w *Worker) commit(ctx context.Context, msg *Message, ev Event) {
	if err := w.source.Commit(ctx, msg); err != nil {
		w.observer.Observe(Event{Kind: EventError, Op: "commit", Message: msg, Attempt: msg.Attempt, Err: err})
		return
	}
	w.observer.Observe(ev)
}

// sleep waits for d or until ctx is done. It reports whether the full delay
// elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
