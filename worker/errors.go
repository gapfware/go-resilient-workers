package worker

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrRetriesExhausted wraps the last handler error when the retry policy
	// gives up.
	ErrRetriesExhausted = errors.New("worker: retries exhausted")

	// ErrAlreadyRunning is returned when Run is called on a running worker.
	ErrAlreadyRunning = errors.New("worker: already running")

	// ErrShutdownTimeout is returned by Run when in-flight handlers did not
	// finish within the configured shutdown timeout.
	ErrShutdownTimeout = errors.New("worker: shutdown timed out with messages in flight")
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable: the message goes straight to the
// dead-letter destination. Use it for validation failures, unknown entities,
// or provider responses that will never succeed on retry.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or any error it wraps) was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type retryAfterError struct {
	err   error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter asks the worker to wait exactly delay before the next attempt,
// overriding the policy's computed backoff (for example to honour a
// provider's Retry-After header). The policy still decides whether another
// attempt is allowed at all.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, delay: delay}
}

func retryAfterDelay(err error) (time.Duration, bool) {
	var r *retryAfterError
	if errors.As(err, &r) {
		return r.delay, true
	}
	return 0, false
}

// PanicError is returned in place of a handler panic. Panics are treated as
// retryable errors so a single bad message cannot take the process down.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("handler panicked: %v", e.Value) }
