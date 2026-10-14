package outbox_test

import (
	"testing"

	"github.com/gapfware/go-resilient-workers/outbox"
	"github.com/gapfware/go-resilient-workers/outbox/outboxtest"
)

func TestMemoryStoreConformance(t *testing.T) {
	outboxtest.Run(t, func(*testing.T) (outbox.Store, outboxtest.EnqueueFunc) {
		s := outbox.NewMemoryStore()
		return s, s.Enqueue
	})
}
