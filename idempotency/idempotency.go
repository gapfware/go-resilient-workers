// Package idempotency guards handlers against processing the same logical
// operation twice when a message is redelivered (at-least-once delivery).
//
// A key is acquired with a lease before the handler runs. On success the key
// is completed and remembered for a TTL; on failure it is released so a later
// retry can acquire it again. If a worker crashes mid-processing the lease
// expires and another worker can take over.
package idempotency

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrAlreadyProcessed is returned by Acquire when the key was completed
	// and is still within its TTL.
	ErrAlreadyProcessed = errors.New("idempotency: key already processed")

	// ErrInProgress is returned by Acquire when another worker holds a
	// non-expired lease on the key.
	ErrInProgress = errors.New("idempotency: key is being processed")
)

// Store persists idempotency keys. Implementations must make Acquire atomic:
// two concurrent callers must never both succeed for the same key.
type Store interface {
	// Acquire reserves key for processing for the duration of lease.
	Acquire(ctx context.Context, key string, lease time.Duration) error
	// Complete marks key as processed and keeps it for ttl.
	Complete(ctx context.Context, key string, ttl time.Duration) error
	// Release drops the lease so the key can be acquired again.
	Release(ctx context.Context, key string) error
}
