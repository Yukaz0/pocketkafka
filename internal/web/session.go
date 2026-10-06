package web

// Dashboard sessions. A signed cookie alone was not enough: it carried no
// expiry the server checked, so a leaked cookie stayed valid until the signing
// secret changed, and logging out only deleted the cookie in the browser. A
// session now has a server-side record with an absolute lifetime, an idle
// timeout, and an id that logout revokes.

import (
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
)

// session is one authenticated dashboard login.
type session struct {
	user    string
	expires time.Time
	idle    time.Time
}

// sessionStore keeps the live sessions of a single dashboard. It is in memory
// on purpose: a restart ends every session, which is the safe direction.
type sessionStore struct {
	mu     sync.Mutex
	byID   map[string]*session
	ttl    time.Duration
	idle   time.Duration
	now    func() time.Time
	lastGC time.Time
}

func newSessionStore(ttl, idle time.Duration) *sessionStore {
	if ttl <= 0 {
		ttl = time.Duration(config.DefaultSessionTTLMinutes) * time.Minute
	}
	if idle <= 0 {
		idle = time.Duration(config.DefaultSessionIdleMinutes) * time.Minute
	}
	if idle > ttl {
		idle = ttl
	}
	return &sessionStore{
		byID: make(map[string]*session),
		ttl:  ttl,
		idle: idle,
		now:  time.Now,
	}
}

// Create starts a session for user and returns its id and expiry.
func (st *sessionStore) Create(user string) (string, time.Time, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", time.Time{}, err
	}
	id := base64.RawURLEncoding.EncodeToString(raw[:])
	now := st.now()
	st.mu.Lock()
	defer st.mu.Unlock()
	st.gcLocked(now)
	st.byID[id] = &session{user: user, expires: now.Add(st.ttl), idle: now.Add(st.idle)}
	return id, now.Add(st.ttl), nil
}

// Validate reports whether a session is still usable for user, refreshing its
// idle deadline. A session that has passed either deadline is dropped: an
// absolute lifetime bounds a stolen cookie, the idle timeout bounds an
// abandoned browser.
func (st *sessionStore) Validate(id, user string) bool {
	if id == "" || user == "" {
		return false
	}
	now := st.now()
	st.mu.Lock()
	defer st.mu.Unlock()
	s, ok := st.byID[id]
	if !ok || s.user != user {
		return false
	}
	if now.After(s.expires) || now.After(s.idle) {
		delete(st.byID, id)
		return false
	}
	s.idle = now.Add(st.idle)
	return true
}

// Revoke ends one session (logout).
func (st *sessionStore) Revoke(id string) {
	if id == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.byID, id)
}

// RevokeUser ends every session of a user, which is what a credential change
// (or an operator reaction to a leak) needs.
func (st *sessionStore) RevokeUser(user string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for id, s := range st.byID {
		if s.user == user {
			delete(st.byID, id)
		}
	}
}

// Active reports the number of live sessions (for the metrics endpoint).
func (st *sessionStore) Active() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return len(st.byID)
}

// gcLocked drops expired sessions; it runs on every create so the map cannot
// grow without bound from logins that are never used again.
func (st *sessionStore) gcLocked(now time.Time) {
	if !st.lastGC.IsZero() && now.Sub(st.lastGC) < time.Minute {
		return
	}
	st.lastGC = now
	for id, s := range st.byID {
		if now.After(s.expires) || now.After(s.idle) {
			delete(st.byID, id)
		}
	}
}

const (
	defaultSessionTTLMinutes  = 1440
	defaultSessionIdleMinutes = 120
)

// sessionPayload renders the cookie payload "user|exp|sid".
func sessionPayload(user string, exp time.Time, id string) string {
	return user + "|" + strconv.FormatInt(exp.Unix(), 10) + "|" + id
}

// parseSessionPayload reads a "user|exp|sid" payload. It rejects the older
// "user|timestamp" form: a cookie issued before sessions existed has no
// revocable id, so it must not be accepted after the upgrade.
func parseSessionPayload(payload string) (user, id string, exp time.Time, ok bool) {
	parts := strings.Split(payload, "|")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return "", "", time.Time{}, false
	}
	secs, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", "", time.Time{}, false
	}
	return parts[0], parts[2], time.Unix(secs, 0), true
}
