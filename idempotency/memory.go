package idempotency

import (
	"context"
	"sync"
	"time"
)

type entryState int

const (
	stateLeased entryState = iota
	stateDone
)

type entry struct {
	state     entryState
	expiresAt time.Time
}

// MemoryStore is an in-process Store. It is safe for concurrent use and is
// suited to tests, single-instance services, or as a reference
// implementation for a Redis/SQL-backed store.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[string]entry
	now     func() time.Time
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]entry), now: time.Now}
}

// Acquire implements Store.
func (s *MemoryStore) Acquire(_ context.Context, key string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	if e, ok := s.entries[key]; ok && now.Before(e.expiresAt) {
		if e.state == stateDone {
			return ErrAlreadyProcessed
		}
		return ErrInProgress
	}
	s.entries[key] = entry{state: stateLeased, expiresAt: now.Add(lease)}
	return nil
}

// Complete implements Store.
func (s *MemoryStore) Complete(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = entry{state: stateDone, expiresAt: s.now().Add(ttl)}
	return nil
}

// Release implements Store.
func (s *MemoryStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && e.state == stateLeased {
		delete(s.entries, key)
	}
	return nil
}

// Sweep removes expired entries and returns how many were dropped. Call it
// periodically in long-running processes to bound memory.
func (s *MemoryStore) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for k, e := range s.entries {
		if !now.Before(e.expiresAt) {
			delete(s.entries, k)
			n++
		}
	}
	return n
}
