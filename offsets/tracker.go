// Package offsets tracks which offsets of a partitioned log can be safely
// committed when messages are processed concurrently and finish out of order.
//
// With Kafka, committing offset N means "everything up to N is done". If
// offsets 10, 11 and 12 are processed in parallel and 12 finishes first,
// committing 12 would silently skip 10 and 11 if the process crashed right
// after. The Tracker only advances the commit point over a contiguous run of
// finished offsets.
package offsets

import "sync"

// Tracker is safe for concurrent use.
type Tracker struct {
	mu    sync.Mutex
	parts map[int]*partition
}

type partition struct {
	order []int64        // fetched, not yet committed offsets in fetch order
	done  map[int64]bool // offsets in order that have finished
	last  int64          // last tracked offset, to detect rewinds
}

// NewTracker returns an empty Tracker.
func NewTracker() *Tracker {
	return &Tracker{parts: make(map[int]*partition)}
}

// Track registers an offset as in flight. Offsets of a partition must be
// tracked in increasing order, as a consumer fetches them. Tracking an
// offset lower than or equal to the previous one is treated as a rewind
// (rebalance or seek) and resets that partition's state.
func (t *Tracker) Track(part int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	p, ok := t.parts[part]
	if !ok || offset <= p.last {
		p = &partition{done: make(map[int64]bool)}
		t.parts[part] = p
	}
	p.order = append(p.order, offset)
	p.done[offset] = false
	p.last = offset
}

// Done marks an offset as finished. It returns the highest offset that can
// now be committed, with ok=false when the commit point did not move (an
// earlier offset is still in flight, or the offset is unknown).
func (t *Tracker) Done(part int, offset int64) (commit int64, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	p, exists := t.parts[part]
	if !exists {
		return 0, false
	}
	if _, tracked := p.done[offset]; !tracked {
		return 0, false
	}
	p.done[offset] = true

	i := 0
	for i < len(p.order) && p.done[p.order[i]] {
		commit = p.order[i]
		delete(p.done, p.order[i])
		i++
	}
	if i == 0 {
		return 0, false
	}
	p.order = p.order[i:]
	return commit, true
}

// Pending returns how many offsets are in flight or waiting for an earlier
// offset to finish, across all partitions.
func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, p := range t.parts {
		n += len(p.order)
	}
	return n
}

// Forget drops all state for a partition, e.g. when it is revoked.
func (t *Tracker) Forget(part int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.parts, part)
}
