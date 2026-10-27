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

type Rule struct {
	Principal    string   `json:"principal"`
	ResourceType string   `json:"resourceType"`
	ResourceName string   `json:"resourceName"`
	Operations   []string `json:"operations"`
}

type Store struct {
	mu    sync.RWMutex
	path  string
	rules []Rule

	allowWildcardAdmin bool
}

func (s *Store) SetAllowWildcardAdmin(allow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allowWildcardAdmin = allow
}

func (s *Store) WildcardAdminRules() []Rule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Rule
	for _, r := range s.rules {
		if grantsWildcardAdmin(r) {
			out = append(out, r)
		}
	}
	return out
}

func grantsWildcardAdmin(rule Rule) bool {
	return rule.Principal == wildcard && containsOperation(rule.Operations, OpAdmin)
}

func (s *Store) wildcardAdminErr(rule Rule) error {
	if !grantsWildcardAdmin(rule) || s.allowWildcardAdmin {
		return nil
	}
	return fmt.Errorf("authz: a rule granting Admin to %q would let every identity administer the cluster; "+
		"grant it to named principals or set security.allow_wildcard_admin=true", wildcard)
}

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

func NewInMemory() *Store { return &Store{} }

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

func (s *Store) Upsert(rule Rule) error {
	if err := ValidateRule(rule); err != nil {
		return err
	}
	s.mu.RLock()
	wildcardErr := s.wildcardAdminErr(rule)
	s.mu.RUnlock()
	if wildcardErr != nil {
		return wildcardErr
	}
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

func (s *Store) Delete(principal, resourceType, resourceName string) error {
	rt, err := normalizeResourceType(resourceType)
	if err != nil {
		return err
	}
	resourceType = rt
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

func (s *Store) Replace(rules []Rule) error {
	s.mu.RLock()
	allowWildcard := s.allowWildcardAdmin
	s.mu.RUnlock()
	for i := range rules {
		if err := ValidateRule(rules[i]); err != nil {
			return err
		}
		if rules[i].Principal == wildcard && !allowWildcard && containsOperation(rules[i].Operations, OpAdmin) {
			return fmt.Errorf("authz: rule for principal %q grants Admin; set security.allow_wildcard_admin=true to accept it", wildcard)
		}
		rt, err := normalizeResourceType(rules[i].ResourceType)
		if err != nil {
			return err
		}
		rules[i].ResourceType = rt
		if strings.TrimSpace(rules[i].ResourceName) == "" {
			rules[i].ResourceName = wildcard
		}
	}
	s.mu.Lock()
	s.rules = append([]Rule(nil), rules...)
	s.mu.Unlock()
	return s.persist()
}

func ValidateRule(rule Rule) error {
	if strings.TrimSpace(rule.Principal) == "" {
		return fmt.Errorf("authz: rule principal must not be empty")
	}
	rt := strings.TrimSpace(rule.ResourceType)
	if rt != "" && !KnownResourceType(strings.ToLower(rt)) {
		return fmt.Errorf("authz: unknown resource type %q (want %q, %q, %q or %q)",
			rule.ResourceType, ResourceTopic, ResourceGroup, ResourceCluster, wildcard)
	}
	for _, op := range rule.Operations {
		if !KnownOperationsSet(strings.TrimSpace(op)) {
			return fmt.Errorf("authz: unknown operation %q", op)
		}
	}
	return nil
}

func (s *Store) Authorize(principal string, op Operation, res Resource) error {
	if principal == "" {
		return &DeniedError{Principal: principal, Operation: op, Resource: res}
	}
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

		return &DeniedError{Principal: principal, Operation: op, Resource: res}
	}
	if wildcardGrant {
		return nil
	}
	return &DeniedError{Principal: principal, Operation: op, Resource: res}
}

func (s *Store) CanDescribeAny(principal, resourceType string) bool {
	if principal == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, r := range s.rules {
		if !principalMatches(r.Principal, principal) {
			continue
		}
		if r.ResourceType != resourceType && r.ResourceType != wildcard {
			continue
		}
		if containsOperation(r.Operations, OpDescribe) {
			return true
		}
	}
	return false
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

func normalizeResourceType(t string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case ResourceTopic:
		return ResourceTopic, nil
	case ResourceGroup:
		return ResourceGroup, nil
	case ResourceCluster:
		return ResourceCluster, nil
	case ResourceFleetServer:
		return ResourceFleetServer, nil
	case ResourceFleetContainer:
		return ResourceFleetContainer, nil
	case ResourceFleetJob:
		return ResourceFleetJob, nil
	case ResourceFleetAudit:
		return ResourceFleetAudit, nil
	case wildcard, "":
		return wildcard, nil
	default:
		return "", fmt.Errorf("authz: unknown resource type %q", t)
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
		for _, granted := range impliedOperations(Operation(o)) {
			if strings.EqualFold(string(granted), string(op)) {
				return true
			}
		}
	}
	return false
}
