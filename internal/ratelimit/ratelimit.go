package ratelimit

import (
	"sync"
	"time"
)

type Limiter struct {
	mu      sync.Mutex
	entries map[string]*entry

	freeAttempts int

	base time.Duration
	max  time.Duration

	ttl time.Duration

	now func() time.Time
}

type entry struct {
	failures int
	next     time.Time
	seen     time.Time
}

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

func (l *Limiter) Succeed(key string) {
	if key == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, key)
}

func (l *Limiter) Failures(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.entries[key]; ok {
		return e.failures
	}
	return 0
}

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

func (l *Limiter) SetClock(now func() time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.now = now
}
