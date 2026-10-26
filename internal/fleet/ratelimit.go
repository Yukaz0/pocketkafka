package fleet

import (
	"sync"
	"time"
)

// RateLimiter tracks request timestamps per key within a sliding window.
type RateLimiter struct {
	mu      sync.Mutex
	limit   int
	window  time.Duration
	clients map[string][]time.Time
}

// NewRateLimiter constructs a RateLimiter with the specified limit and time window.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if limit <= 0 {
		limit = 100
	}
	if window <= 0 {
		window = time.Minute
	}
	return &RateLimiter{
		limit:   limit,
		window:  window,
		clients: make(map[string][]time.Time),
	}
}

// Allow reports whether a request for key is allowed within the configured rate limit.
func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	timestamps := rl.clients[key]
	valid := timestamps[:0]
	for _, t := range timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	if len(valid) >= rl.limit {
		rl.clients[key] = valid
		return false
	}

	rl.clients[key] = append(valid, now)

	// Clean up old entries periodically to prevent memory leaks
	if len(rl.clients) > 1024 {
		for k, v := range rl.clients {
			if len(v) == 0 || v[len(v)-1].Before(cutoff) {
				delete(rl.clients, k)
			}
		}
	}

	return true
}

// Reset clears all recorded request timestamps.
func (rl *RateLimiter) Reset() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.clients = make(map[string][]time.Time)
}
