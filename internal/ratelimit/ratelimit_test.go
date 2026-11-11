package ratelimit

import (
	"fmt"
	"testing"
	"time"
)

func TestBackoffDoublesUpToTheCap(t *testing.T) {
	l := New(2, time.Second, 4*time.Second)
	now := time.Unix(1000, 0)
	l.SetClock(func() time.Time { return now })

	for i := 0; i < 2; i++ {
		if delay := l.Fail("ip:1"); delay != 0 {
			t.Fatalf("failure %d should be free, got %v", i+1, delay)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second}
	for i, w := range want {
		if delay := l.Fail("ip:1"); delay != w {
			t.Fatalf("failure %d delay = %v, want %v", i+3, delay, w)
		}
	}
}

func TestAllowReportsTheRemainingDelay(t *testing.T) {
	l := New(0, time.Second, time.Minute)
	now := time.Unix(2000, 0)
	l.SetClock(func() time.Time { return now })

	if ok, _ := l.Allow("ip:2"); !ok {
		t.Fatal("a fresh key must be allowed")
	}
	l.Fail("ip:2")
	ok, retry := l.Allow("ip:2")
	if ok || retry <= 0 {
		t.Fatalf("Allow after a failure = (%v, %v), want (false, >0)", ok, retry)
	}
	now = now.Add(2 * time.Second)
	if ok, _ := l.Allow("ip:2"); !ok {
		t.Fatal("the key is still blocked after the delay elapsed")
	}
}

func TestSuccessClearsHistory(t *testing.T) {
	l := New(0, time.Second, time.Minute)
	l.Fail("user:app")
	if l.Failures("user:app") == 0 {
		t.Fatal("failure not recorded")
	}
	l.Succeed("user:app")
	if l.Failures("user:app") != 0 {
		t.Fatal("success did not clear the recorded failures")
	}
	if ok, _ := l.Allow("user:app"); !ok {
		t.Fatal("the key is still blocked after a success")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l := New(0, time.Second, time.Minute)
	l.Fail("user:app")
	if ok, _ := l.Allow("user:other"); !ok {
		t.Fatal("one key's backoff leaked onto another key")
	}
	if ok, _ := l.Allow(""); !ok {
		t.Fatal("an empty key must always be allowed")
	}
	if delay := l.Fail(""); delay != 0 {
		t.Fatalf("an empty key recorded a delay %v", delay)
	}
}

func TestStaleKeysArePrunedOnceTheMapGrows(t *testing.T) {
	l := New(0, time.Second, time.Minute)
	now := time.Unix(3000, 0)
	l.SetClock(func() time.Time { return now })

	for i := 0; i < 2000; i++ {
		l.Fail(fmt.Sprintf("ip:%d", i))
	}
	if len(l.entries) != 2000 {
		t.Fatalf("entries = %d, want 2000", len(l.entries))
	}

	now = now.Add(2 * time.Hour)
	if ok, _ := l.Allow("ip:fresh"); !ok {
		t.Fatal("a fresh key must be allowed")
	}
	if len(l.entries) > 2 {
		t.Fatalf("entries = %d after the ttl elapsed, want the stale ones pruned", len(l.entries))
	}
}
