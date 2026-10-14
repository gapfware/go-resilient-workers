// Package outbox implements the transactional outbox pattern.
//
// Writing to a database and publishing an event are two separate systems: if
// the process crashes between them, either the event is lost or it announces
// a change that was rolled back. With an outbox, the event is inserted into
// an outbox table in the same transaction as the business change. A Relay
// then publishes pending records and marks them as published. Delivery is
// at-least-once, so consumers should be idempotent (see the worker package).
package outbox

import (
	"context"
	"time"
)

// Record is an event waiting to be published.
type Record struct {
	ID        string
	Topic     string
	Key       []byte
	Payload   []byte
	Headers   map[string]string
	CreatedAt time.Time
	Attempts  int // failed publish attempts so far
}

// Store holds outbox records.
type Store interface {
	// Claim leases up to limit unpublished records, oldest first, so that
	// concurrent relays never publish the same record at the same time. A
	// lease that is neither marked published nor failed expires after lease.
	//
	// To preserve per-key ordering, Claim must not return a record while an
	// older unpublished record with the same non-empty key is leased or
	// waiting for a retry.
	Claim(ctx context.Context, limit int, lease time.Duration) ([]Record, error)
	// MarkPublished records successful publication.
	MarkPublished(ctx context.Context, ids ...string) error
	// MarkFailed records a failed attempt and makes the record claimable
	// again after retryIn.
	MarkFailed(ctx context.Context, id string, cause error, retryIn time.Duration) error
	// Release drops the lease on records that were claimed but not
	// attempted, without counting a failed attempt.
	Release(ctx context.Context, ids ...string) error
}

// Publisher sends a record to the message broker.
type Publisher interface {
	Publish(ctx context.Context, rec Record) error
}

// PublisherFunc adapts a function to the Publisher interface.
type PublisherFunc func(ctx context.Context, rec Record) error

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, rec Record) error { return f(ctx, rec) }
