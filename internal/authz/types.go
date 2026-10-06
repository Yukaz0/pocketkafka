// Package authz provides the broker's single authorization abstraction. Every
// ingress (Kafka binary protocol, web API, REST proxy, MQTT bridge) routes its
// permission checks through an Authorizer so an ACL enforced in one place is
// enforced everywhere.
package authz

import (
	"fmt"
	"strings"
)

// Operation is an action a principal may attempt.
type Operation string

const (
	// OpDescribe is the right to see that a resource exists and its metadata
	// (topic listing, partition count, group state) without reading any record
	// payload. It is implied by every other operation, so a rule written before
	// OpDescribe existed keeps working unchanged.
	OpDescribe Operation = "Describe"
	OpRead     Operation = "Read"
	OpWrite    Operation = "Write"
	OpAdmin    Operation = "Admin"
)

// KnownOperations lists every operation an ACL rule may name, in the order the
// UI presents them.
var KnownOperations = []Operation{OpDescribe, OpRead, OpWrite, OpAdmin}

// impliedOperations returns the operations a grant of granted confers. Kafka
// semantics: Admin covers everything, Write covers Describe (a producer must
// see the topic to produce), Read covers Describe.
func impliedOperations(granted Operation) []Operation {
	switch granted {
	case OpAdmin:
		return []Operation{OpAdmin, OpRead, OpWrite, OpDescribe}
	case OpWrite:
		return []Operation{OpWrite, OpDescribe}
	case OpRead:
		return []Operation{OpRead, OpDescribe}
	case OpDescribe:
		return []Operation{OpDescribe}
	default:
		return []Operation{granted}
	}
}

// KnownOperationsSet reports whether s is an operation name or "*".
func KnownOperationsSet(s string) bool {
	if s == wildcard {
		return true
	}
	for _, op := range KnownOperations {
		if string(op) == s {
			return true
		}
	}
	return false
}

// Resource identifies the object an operation applies to.
type Resource struct {
	Type string // ResourceTopic | ResourceGroup | ResourceCluster
	Name string
}

// Resource types.
const (
	ResourceTopic   = "topic"
	ResourceGroup   = "group"
	ResourceCluster = "cluster"
)

// KnownResourceType reports whether t names a resource type an ACL rule can
// target (or "*" for any).
func KnownResourceType(t string) bool {
	switch t {
	case ResourceTopic, ResourceGroup, ResourceCluster, wildcard:
		return true
	}
	return false
}

// wildcard matches any principal, resource type, or resource name.
const wildcard = "*"

// AnonymPrincipal is the identity used when authentication is disabled.
const AnonymPrincipal = "anonymous"

// DeniedError reports a refused authorization decision, naming the principal,
// operation, and resource so logs and responses are actionable.
type DeniedError struct {
	Principal string
	Operation Operation
	Resource  Resource
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("authorization denied: principal %q may not %s %s %q",
		e.Principal, e.Operation, e.Resource.Type, e.Resource.Name)
}

// Authorizer decides whether a principal may perform an operation on a resource.
// A nil error means allowed; any error means denied.
type Authorizer interface {
	Authorize(principal string, op Operation, res Resource) error
}

// Lister answers the question a list endpoint asks: may this principal see any
// resource of the given type? A caller that gets true must still filter the
// items it returns, so a scoped grant shows the resources it covers and nothing
// else. An authorizer that does not implement Lister is treated as "ask for
// Describe on the cluster instead".
type Lister interface {
	CanDescribeAny(principal, resourceType string) bool
}

// AllowAllAuthorizer permits everything. It is used when security is disabled so
// the broker keeps its historical allow-all behaviour (decision D2).
type AllowAllAuthorizer struct{}

// Authorize always allows.
func (AllowAllAuthorizer) Authorize(string, Operation, Resource) error { return nil }

// SuperUserAuthorizer grants every operation to a fixed set of principals and
// delegates every other decision to Inner. It is what makes security.enabled
// workable out of the box: an operator names one super user and can then
// administer ACLs through the dashboard instead of hand-editing __acls.json.
type SuperUserAuthorizer struct {
	Inner Authorizer
	Users map[string]struct{}
}

// NewSuperUserAuthorizer wraps inner. An empty user list returns inner
// unchanged, so callers never need to special-case the unconfigured state.
func NewSuperUserAuthorizer(inner Authorizer, users []string) Authorizer {
	if len(users) == 0 {
		return inner
	}
	set := make(map[string]struct{}, len(users))
	for _, u := range users {
		if u = strings.TrimSpace(u); u != "" {
			set[u] = struct{}{}
		}
	}
	if len(set) == 0 {
		return inner
	}
	return &SuperUserAuthorizer{Inner: inner, Users: set}
}

// Authorize grants super users everything and asks Inner about everyone else.
// An empty principal is never a super user.
func (s *SuperUserAuthorizer) Authorize(principal string, op Operation, res Resource) error {
	if principal != "" {
		if _, ok := s.Users[principal]; ok {
			return nil
		}
	}
	return s.Inner.Authorize(principal, op, res)
}

// CanDescribeAny forwards to Inner when Inner can answer it, so a super user
// wraps the store's list behaviour instead of disabling it.
func (s *SuperUserAuthorizer) CanDescribeAny(principal, resourceType string) bool {
	if principal != "" {
		if _, ok := s.Users[principal]; ok {
			return true
		}
	}
	if l, ok := s.Inner.(Lister); ok {
		return l.CanDescribeAny(principal, resourceType)
	}
	return false
}
