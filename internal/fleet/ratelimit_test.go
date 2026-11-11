package fleet

import (
	"testing"
	"time"
)

func TestRateLimiterSlidingWindow(t *testing.T) {
	rl := NewRateLimiter(3, 100*time.Millisecond)

	for range 3 {
		if !rl.Allow("client-a") {
			t.Fatal("expected request within limit to be allowed")
		}
	}

	if rl.Allow("client-a") {
		t.Fatal("expected 4th request to be denied")
	}

	if !rl.Allow("client-b") {
		t.Fatal("expected different key to be allowed")
	}

	time.Sleep(120 * time.Millisecond)

	if !rl.Allow("client-a") {
		t.Fatal("expected request after window expiry to be allowed")
	}
}

func TestRateLimiterReset(t *testing.T) {
	rl := NewRateLimiter(1, time.Minute)

	if !rl.Allow("key") {
		t.Fatal("first call allowed")
	}
	if rl.Allow("key") {
		t.Fatal("second call denied")
	}

	rl.Reset()

	if !rl.Allow("key") {
		t.Fatal("call after reset allowed")
	}
}
