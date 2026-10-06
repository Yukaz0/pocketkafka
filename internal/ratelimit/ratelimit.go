// Package ratelimit provides the failure-driven limiter the broker's
// authentication paths share. A client that guesses wrong is delayed with
// exponential backoff, so an online dictionary attack against a weak password
// costs time instead of succeeding quietly.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter tracks failed attempts per key (a remote address, a username, or
// both) and refuses further attempts until the backoff for that key expires.
// A success clears the key, so a legitimate client that mistyped once is not
// punished afterwards.
type Limiter struct {
	mu      sync.Mutex
	entries map[string]*entry

	// freeAttempts is how many consecutive failures are answered immediately.
	freeAttempts int
	// base is the delay after the first failure past freeAttempts; each further
	// failure doubles it up to max.
	base time.Duration
	max  time.Duration
	// ttl is how long a key's history is remembered; older history is dropped
	// so the map cannot grow without bound.
	ttl time.Duration

	now func() time.Time
}

type entry struct {
	failures int
	next     time.Time
	seen     time.Time
}

// New builds a limiter. Non-positive values fall back to sensible defaults
// (3 free attempts, 1s base, 30s maximum, 15m history).
func New(freeAttempts int, base, max time.Duration) *Limiter {
	if freeAttempts < 0 {
		freeAttempts = 3
	}
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	return &Limiter{
		entries:      make(map[string]*entry),
		freeAttempts: freeAttempts,
		base:         base,
		max:          max,
		ttl:          15 * time.Minute,
		now:          time.Now,
	}
}

// Allow reports whether key may attempt authentication now. When it may not,
// retryAfter is how long the caller should wait (or advertise in a Retry-After
// header). An empty key is always allowed: callers that have no key to limit on
// must not be silently throttled to a standstill.
func (l *Limiter) Allow(key string) (ok bool, retryAfter time.Duration) {
	if key == "" {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked()
	e, found := l.entries[key]
	if !found {
		return true, 0
	}
	now := l.now()
	if now.After(e.next) {
		return true, 0
	}
	return false, e.next.Sub(now)
}

// Fail records a failed attempt for key and returns the resulting delay before
// the next attempt is allowed.
func (l *Limiter) Fail(key string) time.Duration {
	if key == "" {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked()
	e, ok := l.entries[key]
	if !ok {
		e = &entry{}
		l.entries[key] = e
	}
	e.failures++
	e.seen = l.now()
	if e.failures <= l.freeAttempts {
		return 0
	}
	delay := l.base
	for i := l.freeAttempts + 1; i < e.failures; i++ {
		delay *= 2
		if delay >= l.max {
			delay = l.max
			break
		}
	}
	if delay > l.max {
		delay = l.max
	}
	e.next = l.now().Add(delay)
	return delay
}

// Succeed clears the failure history for key.
func (l *Limiter) Succeed(key string) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, key)
}

// Failures reports the consecutive failure count for key (for metrics/logs).
func (l *Limiter) Failures(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[key]; ok {
		return e.failures
	}
	return 0
}

// pruneLocked drops keys whose history has expired. It runs only when the map
// is large enough to matter, keeping the common path allocation-free.
func (l *Limiter) pruneLocked() {
	if len(l.entries) < 1024 {
		return
	}
	cutoff := l.now().Add(-l.ttl)
	for k, e := range l.entries {
		if e.seen.Before(cutoff) && e.next.Before(l.now()) {
			delete(l.entries, k)
		}
	}
}

// SetClock replaces the time source; it exists for tests.
func (l *Limiter) SetClock(now func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.now = now
}
