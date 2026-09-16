package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
)

// actorFrom returns the verified authenticated user for an audit entry, or
// "anonymous".
func (s *Server) actorFrom(r *http.Request) string {
	if u := s.sessionUser(r); u != "" {
		return u
	}
	return "anonymous"
}

// ---------------------------------------------------------------------------
// Fitur 4.4: Schema Registry Studio - expose per-subject schema versions so the
// web UI can run compatibility checks and show a visual diff.
// ---------------------------------------------------------------------------

func (s *Server) handleSchemaDetail(w http.ResponseWriter, r *http.Request) {
	if s.sr == nil {
		writeErr(w, 404, "schema registry disabled")
		return
	}
	subject := r.PathValue("subject")
	versions := s.sr.SubjectVersions(subject)
	if len(versions) == 0 {
		writeErr(w, 404, "subject not found")
		return
	}
	writeJSON(w, 200, versions)
}

// ---------------------------------------------------------------------------
// Fitur 4.5: Visual ACL Manager + Audit Trail (in-memory).
// ---------------------------------------------------------------------------

// ACLRule grants a set of operations on a resource to a principal.
type ACLRule struct {
	Principal    string   `json:"principal"`
	ResourceType string   `json:"resourceType"` // "topic" | "group"
	ResourceName string   `json:"resourceName"`
	Operations   []string `json:"operations"` // "Read","Write","Admin"
}

// AuditEntry is one recorded admin action for the audit trail viewer.
type AuditEntry struct {
	Time     int64  `json:"time"`
	Actor    string `json:"actor"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Detail   string `json:"detail"`
}

// recordAudit appends an audit entry, capped at 500 entries.
func (s *Server) recordAudit(actor, action, resource, detail string) {
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	e := AuditEntry{Time: time.Now().UnixMilli(), Actor: actor, Action: action, Resource: resource, Detail: detail}
	s.auditLog = append(s.auditLog, e)
	if len(s.auditLog) > 500 {
		s.auditLog = s.auditLog[len(s.auditLog)-500:]
	}
}

func (s *Server) handleListACLs(w http.ResponseWriter, r *http.Request) {
	rules := s.aclStore.Rules()
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].Principal != rules[j].Principal {
			return rules[i].Principal < rules[j].Principal
		}
		return rules[i].ResourceName < rules[j].ResourceName
	})
	out := make([]ACLRule, 0, len(rules))
	for _, v := range rules {
		out = append(out, ruleToWire(v))
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleUpsertACL(w http.ResponseWriter, r *http.Request) {
	var rule ACLRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if rule.Principal == "" || rule.ResourceName == "" {
		writeErr(w, 400, "principal and resourceName required")
		return
	}
	if err := s.aclStore.Upsert(ruleFromWire(rule)); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "acl.update", rule.ResourceName, fmt.Sprintf("%s grants %v on %s %s", rule.Principal, rule.Operations, rule.ResourceType, rule.ResourceName))
	writeJSON(w, 200, rule)
}

func (s *Server) handleDeleteACL(w http.ResponseWriter, r *http.Request) {
	var rule ACLRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if err := s.aclStore.Delete(rule.Principal, rule.ResourceType, rule.ResourceName); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "acl.delete", rule.ResourceName, fmt.Sprintf("removed ACL for %s on %s %s", rule.Principal, rule.ResourceType, rule.ResourceName))
	writeJSON(w, 200, map[string]string{"deleted": fmt.Sprintf("%s|%s|%s", rule.Principal, rule.ResourceType, rule.ResourceName)})
}

// ruleFromWire converts the web API representation into an authz rule.
func ruleFromWire(r ACLRule) authz.Rule {
	return authz.Rule{
		Principal:    r.Principal,
		ResourceType: r.ResourceType,
		ResourceName: r.ResourceName,
		Operations:   r.Operations,
	}
}

// ruleToWire converts an authz rule into the web API representation.
func ruleToWire(r authz.Rule) ACLRule {
	return ACLRule{
		Principal:    r.Principal,
		ResourceType: r.ResourceType,
		ResourceName: r.ResourceName,
		Operations:   r.Operations,
	}
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	s.auditMu.Lock()
	out := make([]AuditEntry, len(s.auditLog))
	copy(out, s.auditLog)
	s.auditMu.Unlock()
	// Newest first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	writeJSON(w, 200, out)
}
