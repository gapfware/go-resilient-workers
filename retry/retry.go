// Package retry provides backoff policies used by workers and relays to decide
// whether, and after how long, a failed operation should be attempted again.
package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy decides the delay before the next attempt.
//
// attempt is the number of attempts that have already failed (1 after the
// first failure). Backoff returns ok=false when no further attempts should be
// made.
type Policy interface {
	Backoff(attempt int) (delay time.Duration, ok bool)
}

// PolicyFunc adapts a function to the Policy interface.
type PolicyFunc func(attempt int) (time.Duration, bool)

// Backoff implements Policy.
func (f PolicyFunc) Backoff(attempt int) (time.Duration, bool) { return f(attempt) }

// Exponential is a capped exponential backoff with optional jitter.
//
// The delay after the n-th failure is Initial * Multiplier^(n-1), capped at
// Max. When Jitter is greater than zero the delay is randomly reduced by up to
// that fraction, which spreads retries from many workers over time instead of
// having them hit a recovering dependency at the same instant.
type Exponential struct {
	Initial     time.Duration // delay after the first failure; defaults to 100ms
	Max         time.Duration // upper bound for any single delay; defaults to 30s
	Multiplier  float64       // growth factor between attempts; defaults to 2
	MaxAttempts int           // total attempts including the first one; 0 means unlimited
	Jitter      float64       // fraction in [0,1] of random reduction applied to each delay

	// Rand returns a value in [0,1). It is only overridden in tests.
	Rand func() float64
}

// Backoff implements Policy.
func (e Exponential) Backoff(attempt int) (time.Duration, bool) {
	if attempt < 1 {
		attempt = 1
	}
	if e.MaxAttempts > 0 && attempt >= e.MaxAttempts {
		return 0, false
	}

	initial := e.Initial
	if initial <= 0 {
		initial = 100 * time.Millisecond
	}
	maxDelay := e.Max
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	mult := e.Multiplier
	if mult < 1 {
		mult = 2
	}

	d := float64(initial) * math.Pow(mult, float64(attempt-1))
	if d > float64(maxDelay) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(maxDelay)
	}

	if j := clamp01(e.Jitter); j > 0 {
		r := e.Rand
		if r == nil {
			r = rand.Float64
		}
		d -= d * j * r()
	}
	return time.Duration(d), true
}

// Constant retries with a fixed delay. maxAttempts counts the first attempt;
// 0 means unlimited.
func Constant(delay time.Duration, maxAttempts int) Policy {
	return PolicyFunc(func(attempt int) (time.Duration, bool) {
		if maxAttempts > 0 && attempt >= maxAttempts {
			return 0, false
		}
		return delay, true
	})
}

// Never disables retries: the first failure is final.
func Never() Policy {
	return PolicyFunc(func(int) (time.Duration, bool) { return 0, false })
}

// Default is the policy used when none is configured: 5 attempts, starting at
// 200ms, doubling up to 10s, with 20% jitter.
func Default() Policy {
	return Exponential{
		Initial:     200 * time.Millisecond,
		Max:         10 * time.Second,
		Multiplier:  2,
		MaxAttempts: 5,
		Jitter:      0.2,
	}
}

func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}
