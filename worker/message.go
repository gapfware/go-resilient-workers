// Package worker runs message handlers with retries, dead-lettering,
// idempotency and graceful shutdown on top of any at-least-once message
// source (Kafka, RabbitMQ, SQS, an in-memory queue...).
//
// Delivery contract: a message is committed back to its Source only after it
// was either handled successfully, skipped as a duplicate, or durably sent to
// the dead-letter destination. Anything else (a crash, a shutdown during a
// backoff wait, a failing dead-letter destination) leaves it uncommitted so
// the source redelivers it.
package worker

import (
	"context"
	"errors"
)

// Message is the transport-agnostic unit of work.
type Message struct {
	ID      string            // transport-level identifier, e.g. "topic/partition/offset"
	Topic   string            // logical stream the message came from
	Key     []byte            // partitioning / ordering key
	Value   []byte            // payload
	Headers map[string]string // metadata

	// Attempt is the 1-based number of the current processing attempt. It is
	// set by the worker before each call to the handler.
	Attempt int

	// Raw holds the transport-specific message so adapters can commit it.
	Raw any
}

// Header returns the value of a header, or "" if absent.
func (m *Message) Header(name string) string {
	if m.Headers == nil {
		return ""
	}
	return m.Headers[name]
}

// Handler processes a single message. Returning nil marks it as done; an
// error marked with Permanent sends it straight to the dead-letter
// destination; any other error is retried according to the retry policy.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error { return f(ctx, msg) }

// Middleware wraps a Handler with cross-cutting behaviour (tracing, metrics,
// validation...).
type Middleware func(Handler) Handler

// ErrSourceClosed is returned by Source.Fetch when no more messages will ever
// be delivered. The worker stops gracefully when it sees it.
var ErrSourceClosed = errors.New("worker: source closed")

// Source delivers messages with at-least-once semantics.
type Source interface {
	// Fetch blocks until a message is available or ctx is done.
	Fetch(ctx context.Context) (*Message, error)
	// Commit acknowledges that msg is fully processed. Sources backed by
	// partitioned logs are responsible for only committing contiguous
	// offsets (see the offsets package).
	Commit(ctx context.Context, msg *Message) error
}

// DeadLetter receives messages that failed permanently or exhausted their
// retries.
type DeadLetter interface {
	Send(ctx context.Context, msg *Message, cause error) error
}

// DeadLetterFunc adapts a function to the DeadLetter interface.
type DeadLetterFunc func(ctx context.Context, msg *Message, cause error) error

// Send implements DeadLetter.
func (f DeadLetterFunc) Send(ctx context.Context, msg *Message, cause error) error {
	return f(ctx, msg, cause)
}
