package retry

import (
	"testing"
	"time"
)

func TestExponentialGrowsAndCaps(t *testing.T) {
	p := Exponential{Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second, // capped
		time.Second,
	}
	for i, w := range want {
		got, ok := p.Backoff(i + 1)
		if !ok {
			t.Fatalf("attempt %d: expected retry to be allowed", i+1)
		}
		if got != w {
			t.Errorf("attempt %d: got %v, want %v", i+1, got, w)
		}
	}
}

func TestExponentialStopsAtMaxAttempts(t *testing.T) {
	p := Exponential{Initial: time.Millisecond, MaxAttempts: 3}

	for attempt := 1; attempt < 3; attempt++ {
		if _, ok := p.Backoff(attempt); !ok {
			t.Fatalf("attempt %d should allow a retry", attempt)
		}
	}
	if _, ok := p.Backoff(3); ok {
		t.Fatal("third failure should exhaust a 3-attempt policy")
	}
}

func TestExponentialJitterReducesDelay(t *testing.T) {
	p := Exponential{Initial: time.Second, Max: time.Second, Jitter: 0.5, Rand: func() float64 { return 1 }}

	got, _ := p.Backoff(1)
	if got != 500*time.Millisecond {
		t.Fatalf("got %v, want 500ms with full jitter draw", got)
	}

	p.Rand = func() float64 { return 0 }
	got, _ = p.Backoff(1)
	if got != time.Second {
		t.Fatalf("got %v, want 1s with zero jitter draw", got)
	}
}

func TestExponentialHugeAttemptDoesNotOverflow(t *testing.T) {
	p := Exponential{Initial: time.Second, Max: time.Minute}
	got, ok := p.Backoff(10_000)
	if !ok || got != time.Minute {
		t.Fatalf("got %v ok=%v, want capped 1m", got, ok)
	}
}

func TestConstantAndNever(t *testing.T) {
	c := Constant(50*time.Millisecond, 2)
	if d, ok := c.Backoff(1); !ok || d != 50*time.Millisecond {
		t.Fatalf("constant first retry: got %v ok=%v", d, ok)
	}
	if _, ok := c.Backoff(2); ok {
		t.Fatal("constant policy should stop after 2 attempts")
	}
	if _, ok := Never().Backoff(1); ok {
		t.Fatal("Never must not retry")
	}
}
