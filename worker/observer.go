package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// EventKind identifies what happened to a message.
type EventKind int

const (
	EventReceived     EventKind = iota // fetched from the source
	EventSucceeded                     // handler returned nil and the message was committed
	EventRetrying                      // attempt failed; waiting Delay before the next one
	EventDeadLettered                  // sent to the dead-letter destination and committed
	EventDropped                       // failed for good with no dead-letter destination configured
	EventDuplicate                     // skipped by the idempotency store and committed
	EventAbandoned                     // shutdown interrupted processing; left uncommitted for redelivery
	EventError                         // infrastructure error (fetch, commit, dead-letter, idempotency store)
)

func (k EventKind) String() string {
	switch k {
	case EventReceived:
		return "received"
	case EventSucceeded:
		return "succeeded"
	case EventRetrying:
		return "retrying"
	case EventDeadLettered:
		return "dead_lettered"
	case EventDropped:
		return "dropped"
	case EventDuplicate:
		return "duplicate"
	case EventAbandoned:
		return "abandoned"
	case EventError:
		return "error"
	default:
		return "unknown"
	}
}

// Event describes a step in a message's lifecycle.
type Event struct {
	Kind     EventKind
	Message  *Message      // nil for errors not tied to a message (e.g. fetch errors)
	Attempt  int           // attempt number the event refers to
	Err      error         // cause, when relevant
	Delay    time.Duration // backoff before the next attempt (EventRetrying)
	Duration time.Duration // time since the message was received (terminal events)
	Op       string        // failing operation for EventError: "fetch", "commit", "dead_letter", "idempotency"
}

// Observer receives lifecycle events. Implementations must be safe for
// concurrent use and should not block: they run on the processing path.
type Observer interface {
	Observe(Event)
}

// ObserverFunc adapts a function to the Observer interface.
type ObserverFunc func(Event)

// Observe implements Observer.
func (f ObserverFunc) Observe(e Event) { f(e) }

// Observers fans an event out to several observers.
func Observers(obs ...Observer) Observer {
	return ObserverFunc(func(e Event) {
		for _, o := range obs {
			if o != nil {
				o.Observe(e)
			}
		}
	})
}

// SlogObserver logs every event with structured attributes. Successful and
// received events are logged at Debug, retries at Warn, and terminal
// failures at Error.
func SlogObserver(logger *slog.Logger) Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return ObserverFunc(func(e Event) {
		level := slog.LevelInfo
		switch e.Kind {
		case EventReceived, EventSucceeded, EventDuplicate:
			level = slog.LevelDebug
		case EventRetrying, EventAbandoned:
			level = slog.LevelWarn
		case EventDeadLettered, EventDropped, EventError:
			level = slog.LevelError
		}

		attrs := []slog.Attr{slog.String("event", e.Kind.String())}
		if e.Message != nil {
			attrs = append(attrs,
				slog.String("message_id", e.Message.ID),
				slog.String("topic", e.Message.Topic),
				slog.String("key", string(e.Message.Key)),
			)
		}
		if e.Attempt > 0 {
			attrs = append(attrs, slog.Int("attempt", e.Attempt))
		}
		if e.Op != "" {
			attrs = append(attrs, slog.String("op", e.Op))
		}
		if e.Delay > 0 {
			attrs = append(attrs, slog.Duration("retry_in", e.Delay))
		}
		if e.Duration > 0 {
			attrs = append(attrs, slog.Duration("duration", e.Duration))
		}
		if e.Err != nil {
			attrs = append(attrs, slog.String("error", e.Err.Error()))
		}
		logger.LogAttrs(context.Background(), level, "worker event", attrs...)
	})
}

// Stats counts events. It is a dependency-free starting point for metrics:
// expose its Snapshot through expvar, Prometheus or OpenTelemetry.
type Stats struct {
	received, succeeded, retried, deadLettered, dropped, duplicates, abandoned, errors atomic.Int64
}

// StatsSnapshot is a point-in-time copy of Stats.
type StatsSnapshot struct {
	Received, Succeeded, Retried, DeadLettered, Dropped, Duplicates, Abandoned, Errors int64
}

// Observe implements Observer.
func (s *Stats) Observe(e Event) {
	switch e.Kind {
	case EventReceived:
		s.received.Add(1)
	case EventSucceeded:
		s.succeeded.Add(1)
	case EventRetrying:
		s.retried.Add(1)
	case EventDeadLettered:
		s.deadLettered.Add(1)
	case EventDropped:
		s.dropped.Add(1)
	case EventDuplicate:
		s.duplicates.Add(1)
	case EventAbandoned:
		s.abandoned.Add(1)
	case EventError:
		s.errors.Add(1)
	}
}

// Snapshot returns the current counters.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Received:     s.received.Load(),
		Succeeded:    s.succeeded.Load(),
		Retried:      s.retried.Load(),
		DeadLettered: s.deadLettered.Load(),
		Dropped:      s.dropped.Load(),
		Duplicates:   s.duplicates.Load(),
		Abandoned:    s.abandoned.Load(),
		Errors:       s.errors.Load(),
	}
}
