package outbox

import (
	"context"
	"strconv"
	"sync"
	"time"
)

type memRecord struct {
	Record
	publishedAt time.Time
	lockedUntil time.Time
	lastErr     string
}

// MemoryStore is an in-process Store for tests and examples. It implements
// the same ordering and leasing rules as the Postgres store.
type MemoryStore struct {
	mu      sync.Mutex
	seq     int64
	records []*memRecord
	now     func() time.Time
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{now: time.Now} }

// Enqueue adds a record and returns its ID.
func (s *MemoryStore) Enqueue(_ context.Context, rec Record) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rec.ID = strconv.FormatInt(s.seq, 10)
	rec.CreatedAt = s.now()
	rec.Attempts = 0
	s.records = append(s.records, &memRecord{Record: rec})
	return rec.ID, nil
}

// Claim implements Store.
func (s *MemoryStore) Claim(_ context.Context, limit int, lease time.Duration) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	blockedKeys := make(map[string]bool)
	var out []Record

	for _, r := range s.records {
		if !r.publishedAt.IsZero() {
			continue
		}
		key := string(r.Key)
		locked := now.Before(r.lockedUntil)
		if locked {
			if key != "" {
				blockedKeys[key] = true
			}
			continue
		}
		if key != "" && blockedKeys[key] {
			continue
		}
		if len(out) >= limit {
			break
		}
		r.lockedUntil = now.Add(lease)
		out = append(out, copyRecord(r.Record))
	}
	return out, nil
}

// MarkPublished implements Store.
func (s *MemoryStore) MarkPublished(_ context.Context, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for _, r := range s.find(ids) {
		r.publishedAt = now
		r.lockedUntil = time.Time{}
	}
	return nil
}

// MarkFailed implements Store.
func (s *MemoryStore) MarkFailed(_ context.Context, id string, cause error, retryIn time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.find([]string{id}) {
		r.Attempts++
		r.lastErr = cause.Error()
		r.lockedUntil = s.now().Add(retryIn)
	}
	return nil
}

// Release implements Store.
func (s *MemoryStore) Release(_ context.Context, ids ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.find(ids) {
		if r.publishedAt.IsZero() {
			r.lockedUntil = time.Time{}
		}
	}
	return nil
}

// Pending returns how many records are not yet published.
func (s *MemoryStore) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.records {
		if r.publishedAt.IsZero() {
			n++
		}
	}
	return n
}

func (s *MemoryStore) find(ids []string) []*memRecord {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	var out []*memRecord
	for _, r := range s.records {
		if want[r.ID] {
			out = append(out, r)
		}
	}
	return out
}

func copyRecord(r Record) Record {
	cp := r
	if r.Headers != nil {
		cp.Headers = make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			cp.Headers[k] = v
		}
	}
	return cp
}
