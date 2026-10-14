// Package memory provides in-process implementations of worker.Source and
// worker.DeadLetter. They are meant for tests, examples and local
// development; they do not persist anything.
package memory

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/gapfware/go-resilient-workers/worker"
)

// ErrQueueClosed is returned by Publish after Close.
var ErrQueueClosed = errors.New("memory: queue closed")

// Queue is a buffered, in-memory message source. Messages fetched but never
// committed can be listed with Uncommitted, which mirrors what a durable
// broker would redeliver.
type Queue struct {
	topic  string
	ch     chan *worker.Message
	seq    atomic.Uint64
	closed atomic.Bool
	once   sync.Once

	mu        sync.Mutex
	inflight  map[string]*worker.Message
	committed int
}

// NewQueue returns a queue that holds up to buffer pending messages.
func NewQueue(topic string, buffer int) *Queue {
	if buffer < 0 {
		buffer = 0
	}
	return &Queue{
		topic:    topic,
		ch:       make(chan *worker.Message, buffer),
		inflight: make(map[string]*worker.Message),
	}
}

// Publish enqueues a message, blocking while the buffer is full. ID and Topic
// are filled in when empty.
func (q *Queue) Publish(ctx context.Context, msg *worker.Message) error {
	if q.closed.Load() {
		return ErrQueueClosed
	}
	cp := *msg
	if cp.ID == "" {
		cp.ID = q.topic + "/" + strconv.FormatUint(q.seq.Add(1), 10)
	}
	if cp.Topic == "" {
		cp.Topic = q.topic
	}
	select {
	case q.ch <- &cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting messages. Fetch keeps draining what is buffered and
// then returns worker.ErrSourceClosed. Close must not race with Publish.
func (q *Queue) Close() {
	q.once.Do(func() {
		q.closed.Store(true)
		close(q.ch)
	})
}

// Fetch implements worker.Source.
func (q *Queue) Fetch(ctx context.Context) (*worker.Message, error) {
	select {
	case msg, ok := <-q.ch:
		if !ok {
			return nil, worker.ErrSourceClosed
		}
		q.mu.Lock()
		q.inflight[msg.ID] = msg
		q.mu.Unlock()
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Commit implements worker.Source.
func (q *Queue) Commit(_ context.Context, msg *worker.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inflight[msg.ID]; ok {
		delete(q.inflight, msg.ID)
		q.committed++
	}
	return nil
}

// Committed returns how many messages were committed.
func (q *Queue) Committed() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.committed
}

// Uncommitted returns messages that were fetched but never committed.
func (q *Queue) Uncommitted() []*worker.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]*worker.Message, 0, len(q.inflight))
	for _, m := range q.inflight {
		out = append(out, m)
	}
	return out
}

// DeadLetterEntry is a message captured by DeadLetterQueue.
type DeadLetterEntry struct {
	Message *worker.Message
	Cause   error
}

// DeadLetterQueue records dead-lettered messages in memory.
type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DeadLetterEntry
}

// Send implements worker.DeadLetter.
func (d *DeadLetterQueue) Send(_ context.Context, msg *worker.Message, cause error) error {
	cp := *msg
	d.mu.Lock()
	d.entries = append(d.entries, DeadLetterEntry{Message: &cp, Cause: cause})
	d.mu.Unlock()
	return nil
}

// Entries returns a copy of everything dead-lettered so far.
func (d *DeadLetterQueue) Entries() []DeadLetterEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]DeadLetterEntry(nil), d.entries...)
}
