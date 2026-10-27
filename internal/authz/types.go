package authz

import (
	"fmt"
	"strings"
)

type Operation string

const (
	OpDescribe Operation = "Describe"
	OpRead     Operation = "Read"
	OpWrite    Operation = "Write"
	OpAdmin    Operation = "Admin"
	OpControl  Operation = "Control"
)

var KnownOperations = []Operation{OpDescribe, OpRead, OpWrite, OpAdmin, OpControl}

func impliedOperations(granted Operation) []Operation {
	switch granted {
	case OpAdmin:
		return []Operation{OpAdmin, OpRead, OpWrite, OpDescribe, OpControl}
	case OpControl:
		return []Operation{OpControl, OpDescribe}
	case OpWrite:
		return []Operation{OpWrite, OpDescribe}
	case OpRead:
		return []Operation{OpRead, OpDescribe}
	case OpDescribe:
		return []Operation{OpDescribe}
	default:
		switch strings.ToLower(string(granted)) {
		case "admin":
			return []Operation{OpAdmin, OpRead, OpWrite, OpDescribe, OpControl}
		case "control":
			return []Operation{OpControl, OpDescribe}
		case "write":
			return []Operation{OpWrite, OpDescribe}
		case "read":
			return []Operation{OpRead, OpDescribe}
		case "describe":
			return []Operation{OpDescribe}
		default:
			return []Operation{granted}
		}
	}
}

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

type Resource struct {
	Type string
	Name string
}

const (
	ResourceTopic          = "topic"
	ResourceGroup          = "group"
	ResourceCluster        = "cluster"
	ResourceFleetServer    = "fleet_server"
	ResourceFleetContainer = "fleet_container"
	ResourceFleetJob       = "fleet_job"
	ResourceFleetAudit     = "fleet_audit"
)

func KnownResourceType(t string) bool {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case ResourceTopic, ResourceGroup, ResourceCluster, wildcard:
		return true
	case ResourceFleetServer, ResourceFleetContainer, ResourceFleetJob, ResourceFleetAudit:
		return true
	}
	return false
}

const wildcard = "*"

const AnonymPrincipal = "anonymous"

type DeniedError struct {
	Principal string
	Operation Operation
	Resource  Resource
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("authorization denied: principal %q may not %s %s %q",
		e.Principal, e.Operation, e.Resource.Type, e.Resource.Name)
}

type Authorizer interface {
	Authorize(principal string, op Operation, res Resource) error
}

type Lister interface {
	CanDescribeAny(principal, resourceType string) bool
}

type AllowAllAuthorizer struct{}

func (AllowAllAuthorizer) Authorize(string, Operation, Resource) error { return nil }

type SuperUserAuthorizer struct {
	Inner Authorizer
	Users map[string]struct{}
}

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

func (s *SuperUserAuthorizer) Authorize(principal string, op Operation, res Resource) error {
	if principal != "" {
		if _, ok := s.Users[principal]; ok {
			return nil
		}
	}
	return s.Inner.Authorize(principal, op, res)
}

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
