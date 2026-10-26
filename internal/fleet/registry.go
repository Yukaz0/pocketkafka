package fleet

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
	"github.com/Yukaz0/pocketkafka/internal/credential"
)

var (
	ErrNotFound       = errors.New("fleet: not found")
	ErrRevoked        = errors.New("fleet: agent revoked")
	ErrTokenInvalid   = errors.New("fleet: enrollment token invalid")
	ErrTokenUsed      = errors.New("fleet: enrollment token already consumed")
	ErrTokenExpired   = errors.New("fleet: enrollment token expired")
	ErrServerRequired = errors.New("fleet: server ID required")
)

type registryData struct {
	Agents      []*Agent      `json:"agents"`
	Enrollments []*Enrollment `json:"enrollments"`
	Servers     []*Server     `json:"servers,omitempty"`
}

// Registry manages servers, agents, and single-use enrollment tokens.
// Safe for concurrent access. Persists state via atomicfile.
type Registry struct {
	mu          sync.RWMutex
	path        string
	agents      map[string]*Agent
	enrollments map[string]*Enrollment
	servers     map[string]*Server
}

// NewRegistry constructs a Registry bound to dirOrPath.
// If dirOrPath is a directory, state lives in __fleet_agents.json.
// If dirOrPath is empty, state is in-memory only.
func NewRegistry(dirOrPath string) *Registry {
	r := &Registry{
		agents:      make(map[string]*Agent),
		enrollments: make(map[string]*Enrollment),
		servers:     make(map[string]*Server),
	}
	if dirOrPath == "" {
		return r
	}
	if strings.HasSuffix(dirOrPath, ".json") {
		r.path = dirOrPath
	} else {
		r.path = filepath.Join(dirOrPath, "__fleet_agents.json")
	}
	_ = r.load()
	return r
}

func (r *Registry) load() error {
	if r.path == "" {
		return nil
	}
	data, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("fleet: read registry: %w", err)
	}
	if len(data) == 0 {
		return nil
	}

	var state registryData
	if err := json.Unmarshal(data, &state); err != nil {
		// Fallback for flat agent list
		var agents []*Agent
		if err2 := json.Unmarshal(data, &agents); err2 == nil {
			for _, a := range agents {
				r.agents[a.ID] = a
			}
			return nil
		}
		return fmt.Errorf("fleet: parse registry: %w", err)
	}

	for _, a := range state.Agents {
		r.agents[a.ID] = a
	}
	for _, e := range state.Enrollments {
		r.enrollments[e.Token] = e
	}
	for _, s := range state.Servers {
		r.servers[s.ID] = s
	}
	return nil
}

func (r *Registry) persistLocked() error {
	if r.path == "" {
		return nil
	}
	state := registryData{
		Agents:      make([]*Agent, 0, len(r.agents)),
		Enrollments: make([]*Enrollment, 0, len(r.enrollments)),
		Servers:     make([]*Server, 0, len(r.servers)),
	}
	for _, a := range r.agents {
		state.Agents = append(state.Agents, a)
	}
	for _, e := range r.enrollments {
		state.Enrollments = append(state.Enrollments, e)
	}
	for _, s := range r.servers {
		state.Servers = append(state.Servers, s)
	}

	sort.Slice(state.Agents, func(i, j int) bool { return state.Agents[i].ID < state.Agents[j].ID })
	sort.Slice(state.Enrollments, func(i, j int) bool { return state.Enrollments[i].Token < state.Enrollments[j].Token })
	sort.Slice(state.Servers, func(i, j int) bool { return state.Servers[i].ID < state.Servers[j].ID })

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("fleet: marshal registry: %w", err)
	}
	return atomicfile.Write(r.path, data, 0o600)
}

// Enroll issues a 32-byte single-use token expiring in 15 minutes.
func (r *Registry) Enroll(serverID string) (string, error) {
	return r.enrollWithExpiry(serverID, 15*time.Minute)
}

func (r *Registry) enrollWithExpiry(serverID string, duration time.Duration) (string, error) {
	if strings.TrimSpace(serverID) == "" {
		return "", ErrServerRequired
	}
	tokBytes := make([]byte, 32)
	if _, err := rand.Read(tokBytes); err != nil {
		return "", fmt.Errorf("fleet: generate token: %w", err)
	}
	token := hex.EncodeToString(tokBytes)

	now := time.Now()
	e := &Enrollment{
		Token:     token,
		ServerID:  serverID,
		CreatedAt: now,
		ExpiresAt: now.Add(duration),
		Used:      false,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.enrollments[token] = e
	if err := r.persistLocked(); err != nil {
		return "", err
	}
	return token, nil
}

// ConsumeToken redeems an enrollment token exactly once, returning a newly registered Agent.
func (r *Registry) ConsumeToken(token string) (*Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.enrollments[token]
	if !ok {
		return nil, ErrTokenInvalid
	}
	if e.Used {
		return nil, ErrTokenUsed
	}
	if time.Now().After(e.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return nil, fmt.Errorf("fleet: generate secret: %w", err)
	}
	rawSecret := hex.EncodeToString(rawBytes)

	secretHash, err := credential.HashPassword(rawSecret)
	if err != nil {
		return nil, fmt.Errorf("fleet: hash secret: %w", err)
	}

	agentBytes := make([]byte, 8)
	if _, err := rand.Read(agentBytes); err != nil {
		return nil, fmt.Errorf("fleet: generate agent id: %w", err)
	}
	agentID := fmt.Sprintf("agent-%s-%s", e.ServerID, hex.EncodeToString(agentBytes))

	now := time.Now()
	e.Used = true
	e.UsedAt = now

	ag := &Agent{
		ID:         agentID,
		ServerID:   e.ServerID,
		Secret:     secretHash,
		Revoked:    false,
		EnrolledAt: now,
	}

	r.agents[agentID] = ag
	if err := r.persistLocked(); err != nil {
		return nil, err
	}
	cp := *ag
	cp.RawSecret = rawSecret
	return &cp, nil
}

// Revoke disables an agent immediately.
func (r *Registry) Revoke(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ag, ok := r.agents[agentID]
	if !ok {
		return fmt.Errorf("fleet: agent %q not found: %w", agentID, ErrNotFound)
	}
	if ag.Revoked {
		return nil
	}
	ag.Revoked = true
	ag.RevokedAt = time.Now()
	return r.persistLocked()
}

// Lookup finds an agent by ID.
func (r *Registry) Lookup(agentID string) (*Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ag, ok := r.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("fleet: agent %q not found: %w", agentID, ErrNotFound)
	}
	cp := *ag
	cp.RawSecret = ""
	return &cp, nil
}

// AuthorizeStream checks whether the agent exists and is not revoked.
func (r *Registry) AuthorizeStream(agentID string) (*Agent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ag, ok := r.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("fleet: agent %q not found: %w", agentID, ErrNotFound)
	}
	if ag.Revoked {
		return nil, fmt.Errorf("fleet: agent %q is revoked: %w", agentID, ErrRevoked)
	}
	cp := *ag
	cp.RawSecret = ""
	return &cp, nil
}

// ListAgents returns all agents sorted by ID.
func (r *Registry) ListAgents() []*Agent {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*Agent, 0, len(r.agents))
	for _, a := range r.agents {
		cp := *a
		cp.RawSecret = ""
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
