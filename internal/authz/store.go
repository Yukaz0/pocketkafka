package authz

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
)

// Rule grants a set of operations on a resource to a principal. ResourceType,
// ResourceName, and Principal may be "*" to match anything.
type Rule struct {
	Principal    string   `json:"principal"`
	ResourceType string   `json:"resourceType"`
	ResourceName string   `json:"resourceName"`
	Operations   []string `json:"operations"`
}

// Store is a file-backed ACL store. It is safe for concurrent use. A store with
// an empty rule set denies everything (default-deny, decision D2); callers that
// want allow-all must use AllowAllAuthorizer.
type Store struct {
	mu    sync.RWMutex
	path  string // empty for an in-memory store
	rules []Rule
}

// NewStore loads the ACL file at path. A missing file yields an empty
// (deny-all) store. A corrupt file is an error: silently degrading a malformed
// ACL set into "no rules" would turn a typo into an outage, and degrading into
// "allow all" would be a security hole.
func NewStore(path string) (*Store, error) {
	s := &Store{path: path}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("authz: read %s: %w", path, err)
	}
	if len(data) == 0 {
		return s, nil
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("authz: parse %s: %w", path, err)
	}
	s.rules = rules
	return s, nil
}

// NewInMemory returns a store that never persists, useful for tests.
func NewInMemory() *Store { return &Store{} }

// Rules returns a copy of the current rule set, sorted for stable output.
func (s *Store) Rules() []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Rule, len(s.rules))
	copy(out, s.rules)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Principal != out[j].Principal {
			return out[i].Principal < out[j].Principal
		}
		if out[i].ResourceType != out[j].ResourceType {
			return out[i].ResourceType < out[j].ResourceType
		}
		return out[i].ResourceName < out[j].ResourceName
	})
	return out
}

// Upsert inserts or replaces the rule keyed by principal/resourceType/resourceName.
func (s *Store) Upsert(rule Rule) error {
	if strings.TrimSpace(rule.Principal) == "" {
		return fmt.Errorf("authz: rule principal must not be empty")
	}
	rule.ResourceType = normalizeResourceType(rule.ResourceType)
	if strings.TrimSpace(rule.ResourceName) == "" {
		rule.ResourceName = wildcard
	}
	s.mu.Lock()
	replaced := false
	for i := range s.rules {
		if sameKey(s.rules[i], rule) {
			s.rules[i] = rule
			replaced = true
			break
		}
	}
	if !replaced {
		s.rules = append(s.rules, rule)
	}
	s.mu.Unlock()
	return s.persist()
}

// Delete removes the rule for the given key if present.
func (s *Store) Delete(principal, resourceType, resourceName string) error {
	resourceType = normalizeResourceType(resourceType)
	s.mu.Lock()
	kept := s.rules[:0:0]
	for _, r := range s.rules {
		if r.Principal == principal && r.ResourceType == resourceType && r.ResourceName == resourceName {
			continue
		}
		kept = append(kept, r)
	}
	s.rules = kept
	s.mu.Unlock()
	return s.persist()
}

// Replace swaps the entire rule set and persists it.
func (s *Store) Replace(rules []Rule) error {
	for i := range rules {
		rules[i].ResourceType = normalizeResourceType(rules[i].ResourceType)
		if strings.TrimSpace(rules[i].ResourceName) == "" {
			rules[i].ResourceName = wildcard
		}
	}
	s.mu.Lock()
	s.rules = append([]Rule(nil), rules...)
	s.mu.Unlock()
	return s.persist()
}

// Authorize implements Authorizer with default-deny and deterministic
// precedence: if any rule matches the resource exactly (both type and name),
// only exact rules can grant; wildcard rules are consulted only when no exact
// rule matches.
func (s *Store) Authorize(principal string, op Operation, res Resource) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	exactExists := false
	wildcardGrant := false
	for _, r := range s.rules {
		if !principalMatches(r.Principal, principal) {
			continue
		}
		if r.ResourceType != res.Type && r.ResourceType != wildcard {
			continue
		}
		if r.ResourceName != res.Name && r.ResourceName != wildcard {
			continue
		}
		grant := containsOperation(r.Operations, op)
		if r.ResourceType == res.Type && r.ResourceName == res.Name {
			exactExists = true
			if grant {
				return nil
			}
			continue
		}
		wildcardGrant = wildcardGrant || grant
	}
	if exactExists {
		// Exact rules exist for this resource but none granted the operation.
		return &DeniedError{Principal: principal, Operation: op, Resource: res}
	}
	if wildcardGrant {
		return nil
	}
	return &DeniedError{Principal: principal, Operation: op, Resource: res}
}

func (s *Store) persist() error {
	if s.path == "" {
		return nil
	}
	s.mu.RLock()
	data, err := json.MarshalIndent(s.rules, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("authz: marshal: %w", err)
	}
	return atomicfile.Write(s.path, data, 0o600)
}

func sameKey(a, b Rule) bool {
	return a.Principal == b.Principal && a.ResourceType == b.ResourceType && a.ResourceName == b.ResourceName
}

func normalizeResourceType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case ResourceTopic:
		return ResourceTopic
	case ResourceGroup:
		return ResourceGroup
	case ResourceCluster:
		return ResourceCluster
	case wildcard, "":
		return wildcard
	default:
		return strings.ToLower(strings.TrimSpace(t))
	}
}

func principalMatches(rulePrincipal, principal string) bool {
	return rulePrincipal == principal || rulePrincipal == wildcard
}

func containsOperation(ops []string, op Operation) bool {
	for _, o := range ops {
		if o == wildcard {
			return true
		}
		if Operation(o) == op {
			return true
		}
	}
	return false
}
