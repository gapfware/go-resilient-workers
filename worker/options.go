package worker

import (
	"time"

	"github.com/gapfware/go-resilient-workers/idempotency"
	"github.com/gapfware/go-resilient-workers/retry"
)

// Option configures a Worker.
type Option func(*Worker)

// WithConcurrency sets how many messages are processed in parallel
// (default 1).
func WithConcurrency(n int) Option {
	return func(w *Worker) {
		if n > 0 {
			w.concurrency = n
		}
	}
}

// WithKeyOrdering guarantees that messages sharing a Key are processed one at
// a time and in fetch order, even with concurrency > 1. Messages are routed to
// a fixed lane by hashing their key. The trade-off is head-of-line blocking:
// a message waiting for a retry delays the others in its lane.
func WithKeyOrdering() Option {
	return func(w *Worker) { w.keyOrdered = true }
}

// WithRetryPolicy sets the backoff policy for handler failures
// (default retry.Default()).
func WithRetryPolicy(p retry.Policy) Option {
	return func(w *Worker) {
		if p != nil {
			w.policy = p
		}
	}
}

// WithDeadLetter sets where permanently failing messages go. Without one, a
// message that exhausts its retries is committed and reported as
// EventDropped.
func WithDeadLetter(d DeadLetter) Option {
	return func(w *Worker) { w.deadLetter = d }
}

// WithDeadLetterRetryPolicy controls how sending to the dead-letter
// destination is retried. By default it is retried indefinitely (capped
// exponential backoff) until it succeeds or the worker shuts down, because
// committing a message that never reached the DLQ would lose it.
func WithDeadLetterRetryPolicy(p retry.Policy) Option {
	return func(w *Worker) {
		if p != nil {
			w.dlqPolicy = p
		}
	}
}

// KeyFunc derives the idempotency key of a message. Returning "" disables
// the idempotency check for that message.
type KeyFunc func(*Message) string

// KeyFromHeader uses the value of a header as idempotency key.
func KeyFromHeader(name string) KeyFunc {
	return func(m *Message) string { return m.Header(name) }
}

// KeyFromMessageKey uses the message key as idempotency key.
func KeyFromMessageKey() KeyFunc {
	return func(m *Message) string { return string(m.Key) }
}

// WithIdempotency skips messages whose key was already processed. lease bounds
// how long a crashed worker can block a key; ttl is how long completed keys
// are remembered (it should exceed the source's redelivery window).
func WithIdempotency(store idempotency.Store, key KeyFunc, lease, ttl time.Duration) Option {
	return func(w *Worker) {
		if store == nil || key == nil {
			return
		}
		if lease <= 0 {
			lease = time.Minute
		}
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		w.idem = &idemConfig{store: store, key: key, lease: lease, ttl: ttl}
	}
}

// WithHandlerTimeout bounds each handler attempt. A timed-out attempt counts
// as a retryable failure.
func WithHandlerTimeout(d time.Duration) Option {
	return func(w *Worker) { w.handlerTimeout = d }
}

// WithShutdownTimeout bounds how long Run waits for in-flight handlers after
// its context is cancelled (default 30s). When it elapses the handlers'
// context is cancelled and Run returns ErrShutdownTimeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(w *Worker) {
		if d > 0 {
			w.shutdownTimeout = d
		}
	}
}

// WithObserver receives lifecycle events (logging, metrics, tracing). It can
// be passed several times; every observer receives every event.
func WithObserver(o Observer) Option {
	return func(w *Worker) {
		if o != nil {
			w.observers = append(w.observers, o)
		}
	}
}

// WithMiddleware wraps the handler. The first middleware is the outermost.
func WithMiddleware(mw ...Middleware) Option {
	return func(w *Worker) { w.middleware = append(w.middleware, mw...) }
}

// WithFetchErrorBackoff sets the pause after a failed Fetch (default 1s).
func WithFetchErrorBackoff(d time.Duration) Option {
	return func(w *Worker) {
		if d > 0 {
			w.fetchBackoff = d
		}
	}
}

type idemConfig struct {
	store      idempotency.Store
	key        KeyFunc
	lease, ttl time.Duration
}
