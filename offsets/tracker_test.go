package offsets

import (
	"math/rand/v2"
	"sync"
	"testing"
)

func TestOutOfOrderCompletionOnlyCommitsContiguousRun(t *testing.T) {
	tr := NewTracker()
	for off := int64(10); off <= 13; off++ {
		tr.Track(0, off)
	}

	if _, ok := tr.Done(0, 12); ok {
		t.Fatal("12 finished before 10 and 11: must not commit")
	}
	if _, ok := tr.Done(0, 11); ok {
		t.Fatal("10 still in flight: must not commit")
	}
	commit, ok := tr.Done(0, 10)
	if !ok || commit != 12 {
		t.Fatalf("got commit=%d ok=%v, want 12 (10,11,12 all done)", commit, ok)
	}
	commit, ok = tr.Done(0, 13)
	if !ok || commit != 13 {
		t.Fatalf("got commit=%d ok=%v, want 13", commit, ok)
	}
	if tr.Pending() != 0 {
		t.Fatalf("pending=%d, want 0", tr.Pending())
	}
}

func TestPartitionsAreIndependent(t *testing.T) {
	tr := NewTracker()
	tr.Track(0, 1)
	tr.Track(1, 100)

	if c, ok := tr.Done(1, 100); !ok || c != 100 {
		t.Fatalf("partition 1: got %d %v", c, ok)
	}
	if c, ok := tr.Done(0, 1); !ok || c != 1 {
		t.Fatalf("partition 0: got %d %v", c, ok)
	}
}

func TestRewindResetsPartition(t *testing.T) {
	tr := NewTracker()
	tr.Track(0, 50)
	tr.Track(0, 51)
	// Rebalance: consumer restarts from the last committed offset.
	tr.Track(0, 40)

	if _, ok := tr.Done(0, 50); ok {
		t.Fatal("offset from before the rewind must be ignored")
	}
	if c, ok := tr.Done(0, 40); !ok || c != 40 {
		t.Fatalf("got %d %v, want 40", c, ok)
	}
}

func TestUnknownOffsetIsIgnored(t *testing.T) {
	tr := NewTracker()
	if _, ok := tr.Done(3, 7); ok {
		t.Fatal("unknown partition must not commit")
	}
	tr.Track(3, 7)
	if _, ok := tr.Done(3, 8); ok {
		t.Fatal("untracked offset must not commit")
	}
}

func TestConcurrentRandomCompletionEndsAtLastOffset(t *testing.T) {
	const n = 2000
	tr := NewTracker()
	for i := int64(0); i < n; i++ {
		tr.Track(0, i)
	}

	perm := rand.Perm(n)
	var mu sync.Mutex
	var maxCommit int64 = -1
	var wg sync.WaitGroup
	for _, p := range perm {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			if c, ok := tr.Done(0, off); ok {
				mu.Lock()
				if c > maxCommit {
					maxCommit = c
				}
				mu.Unlock()
			}
		}(int64(p))
	}
	wg.Wait()

	if maxCommit != n-1 {
		t.Fatalf("final commit %d, want %d", maxCommit, n-1)
	}
	if tr.Pending() != 0 {
		t.Fatalf("pending=%d, want 0", tr.Pending())
	}
}
