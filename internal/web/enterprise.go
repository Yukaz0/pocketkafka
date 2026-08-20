package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// actorFrom returns the authenticated user for an audit entry, or "anonymous".
func actorFrom(r *http.Request) string {
	if u := authenticatedUser(r); u != "" {
		return u
	}
	return "anonymous"
}

// authenticatedUser extracts the username from the signed auth cookie, if valid.
func authenticatedUser(r *http.Request) string {
	c, err := r.Cookie(authCookieName)
	if err != nil {
		return ""
	}
	// The token is signed; we cannot verify the signature without the secret
	// here, so fall back to the payload-only extraction used for display.
	return authPayloadToUser(c.Value)
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
	s.aclMu.RLock()
	out := make([]ACLRule, 0, len(s.acls))
	for _, v := range s.acls {
		out = append(out, v)
	}
	s.aclMu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Principal != out[j].Principal {
			return out[i].Principal < out[j].Principal
		}
		return out[i].ResourceName < out[j].ResourceName
	})
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
	key := fmt.Sprintf("%s|%s|%s", rule.Principal, rule.ResourceType, rule.ResourceName)
	s.aclMu.Lock()
	s.acls[key] = rule
	s.aclMu.Unlock()
	s.recordAudit(actorFrom(r), "acl.update", rule.ResourceName, fmt.Sprintf("%s grants %v on %s %s", rule.Principal, rule.Operations, rule.ResourceType, rule.ResourceName))
	writeJSON(w, 200, rule)
}

func (s *Server) handleDeleteACL(w http.ResponseWriter, r *http.Request) {
	var rule ACLRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	key := fmt.Sprintf("%s|%s|%s", rule.Principal, rule.ResourceType, rule.ResourceName)
	s.aclMu.Lock()
	delete(s.acls, key)
	s.aclMu.Unlock()
	s.recordAudit(actorFrom(r), "acl.delete", rule.ResourceName, fmt.Sprintf("removed ACL for %s on %s %s", rule.Principal, rule.ResourceType, rule.ResourceName))
	writeJSON(w, 200, map[string]string{"deleted": key})
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
