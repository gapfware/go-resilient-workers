package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gapfware/go-resilient-workers/retry"
)

// Relay moves records from a Store to a Publisher.
//
// Records with the same key are published in insertion order: when one
// fails, the remaining records of that key in the batch are released and the
// store keeps them blocked until the failed one is published.
type Relay struct {
	store     Store
	publisher Publisher

	interval  time.Duration
	batchSize int
	lease     time.Duration
	policy    retry.Policy
	onError   func(error)
}

// RelayOption configures a Relay.
type RelayOption func(*Relay)

// WithPollInterval sets how often the store is polled when idle (default 1s).
func WithPollInterval(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithBatchSize sets how many records are claimed per poll (default 100).
func WithBatchSize(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.batchSize = n
		}
	}
}

// WithLease sets how long claimed records stay reserved for this relay
// (default 30s). It must exceed the time needed to publish a batch.
func WithLease(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.lease = d
		}
	}
}

// WithRelayRetryPolicy decides how long a failed record waits before it is
// claimable again, based on its number of failed attempts. Records are never
// discarded: when the policy gives up, the record waits for its Max delay (or
// one hour) and the error handler should alert on it.
func WithRelayRetryPolicy(p retry.Policy) RelayOption {
	return func(r *Relay) {
		if p != nil {
			r.policy = p
		}
	}
}

// WithErrorHandler is called for every store or publish error. Errors from
// Publish are wrapped in *PublishError.
func WithErrorHandler(fn func(error)) RelayOption {
	return func(r *Relay) {
		if fn != nil {
			r.onError = fn
		}
	}
}

// PublishError reports a record that could not be published.
type PublishError struct {
	Record  Record
	Err     error
	RetryIn time.Duration
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("outbox: publish record %s (topic %s, attempt %d): %v", e.Record.ID, e.Record.Topic, e.Record.Attempts+1, e.Err)
}

func (e *PublishError) Unwrap() error { return e.Err }

// NewRelay builds a Relay.
func NewRelay(store Store, publisher Publisher, opts ...RelayOption) *Relay {
	r := &Relay{
		store:     store,
		publisher: publisher,
		interval:  time.Second,
		batchSize: 100,
		lease:     30 * time.Second,
		policy:    retry.Exponential{Initial: time.Second, Max: 5 * time.Minute, Jitter: 0.2},
		onError:   func(error) {},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run polls until ctx is cancelled. When a poll returns a full batch the next
// one starts immediately, so a backlog drains at full speed.
func (r *Relay) Run(ctx context.Context) error {
	for {
		n, err := r.Flush(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.onError(err)
		}
		if err == nil && n >= r.batchSize {
			continue
		}
		t := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// Flush runs a single claim/publish cycle and returns how many records were
// claimed.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	recs, err := r.store.Claim(ctx, r.batchSize, r.lease)
	if err != nil {
		return 0, fmt.Errorf("outbox: claim: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}

	var (
		published []string
		skipped   []string
		errs      []error
		blocked   = make(map[string]bool)
	)

	for _, rec := range recs {
		key := string(rec.Key)
		if (key != "" && blocked[key]) || ctx.Err() != nil {
			skipped = append(skipped, rec.ID)
			continue
		}

		if err := r.publisher.Publish(ctx, rec); err != nil {
			retryIn := r.retryDelay(rec.Attempts + 1)
			pe := &PublishError{Record: rec, Err: err, RetryIn: retryIn}
			r.onError(pe)
			if merr := r.store.MarkFailed(ctx, rec.ID, err, retryIn); merr != nil {
				errs = append(errs, fmt.Errorf("outbox: mark failed %s: %w", rec.ID, merr))
			}
			if key != "" {
				blocked[key] = true
			}
			continue
		}
		published = append(published, rec.ID)
	}

	// Use a context that survives cancellation: records already sent must
	// be recorded, or they will be published again.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	if len(published) > 0 {
		if err := r.store.MarkPublished(finishCtx, published...); err != nil {
			errs = append(errs, fmt.Errorf("outbox: mark published: %w", err))
		}
	}
	if len(skipped) > 0 {
		if err := r.store.Release(finishCtx, skipped...); err != nil {
			errs = append(errs, fmt.Errorf("outbox: release: %w", err))
		}
	}
	return len(recs), errors.Join(errs...)
}

func (r *Relay) retryDelay(attempt int) time.Duration {
	if d, ok := r.policy.Backoff(attempt); ok {
		return d
	}
	if e, ok := r.policy.(retry.Exponential); ok && e.Max > 0 {
		return e.Max
	}
	return time.Hour
}
