// Package authz provides the broker's single authorization abstraction. Every
// ingress (Kafka binary protocol, web API, REST proxy, MQTT bridge) routes its
// permission checks through an Authorizer so an ACL enforced in one place is
// enforced everywhere.
package authz

import "fmt"

// Operation is an action a principal may attempt.
type Operation string

const (
	OpRead  Operation = "Read"
	OpWrite Operation = "Write"
	OpAdmin Operation = "Admin"
)

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

// AllowAllAuthorizer permits everything. It is used when security is disabled so
// the broker keeps its historical allow-all behaviour (decision D2).
type AllowAllAuthorizer struct{}

// Authorize always allows.
func (AllowAllAuthorizer) Authorize(string, Operation, Resource) error { return nil }
