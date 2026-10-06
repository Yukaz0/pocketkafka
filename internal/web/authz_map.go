package web

// Per-route authorization for the dashboard API. The previous rule — "GET is
// always allowed, every mutation needs cluster Write" — let any authenticated
// user read every topic, the audit trail and the broker log, and let a single
// cluster Write grant reach every destructive endpoint. Each route now names
// the operation and resource it needs, and a route that is missing from the
// table is denied (fail closed), so a new endpoint cannot quietly skip the
// check.

import (
	"strings"

	"github.com/Yukaz0/pocketkafka/internal/authz"
)

// clusterResource is the resource for actions that affect the whole broker.
var clusterResource = authz.Resource{Type: authz.ResourceCluster, Name: "cluster"}

// apiRule is the permission one dashboard route requires.
type apiRule struct {
	op authz.Operation
	// list marks an endpoint that returns many resources. It is allowed when
	// the principal can Describe any resource of listType, and the handler
	// filters the response to what that principal may see.
	list     bool
	listType string
	// res resolves the resource from the matched path parameters.
	res func(params map[string]string) authz.Resource
}

func clusterScoped(map[string]string) authz.Resource { return clusterResource }

func topicScoped(params map[string]string) authz.Resource {
	return authz.Resource{Type: authz.ResourceTopic, Name: params["topic"]}
}

func groupScoped(params map[string]string) authz.Resource {
	return authz.Resource{Type: authz.ResourceGroup, Name: params["group"]}
}

// apiRules maps each route pattern registered in Handler to the permission it
// requires.
var apiRules = map[string]apiRule{
	"GET /api/v1/cluster": {op: authz.OpDescribe, res: clusterScoped},

	"GET /api/v1/topics":                         {op: authz.OpDescribe, list: true, listType: authz.ResourceTopic, res: clusterScoped},
	"POST /api/v1/topics":                        {op: authz.OpAdmin, res: clusterScoped},
	"DELETE /api/v1/topics/{topic}":              {op: authz.OpAdmin, res: topicScoped},
	"GET /api/v1/topics/{topic}/messages":        {op: authz.OpRead, res: topicScoped},
	"POST /api/v1/topics/{topic}/messages":       {op: authz.OpWrite, res: topicScoped},
	"GET /api/v1/topics/{topic}/partitions":      {op: authz.OpDescribe, res: topicScoped},
	"POST /api/v1/topics/{topic}/truncate":       {op: authz.OpAdmin, res: topicScoped},
	"POST /api/v1/topics/{topic}/compact":        {op: authz.OpAdmin, res: topicScoped},
	"GET /api/v1/topics/{topic}/config":          {op: authz.OpDescribe, res: topicScoped},
	"PUT /api/v1/topics/{topic}/config":          {op: authz.OpAdmin, res: topicScoped},
	"POST /api/v1/topics/{topic}/import":         {op: authz.OpWrite, res: topicScoped},
	"GET /api/v1/topics/{topic}/tail":            {op: authz.OpRead, res: topicScoped},
	"GET /api/v1/groups":                         {op: authz.OpDescribe, list: true, listType: authz.ResourceGroup, res: clusterScoped},
	"GET /api/v1/groups/{group}":                 {op: authz.OpDescribe, res: groupScoped},
	"DELETE /api/v1/groups/{group}":              {op: authz.OpAdmin, res: groupScoped},
	"POST /api/v1/groups/{group}/offsets/reset":  {op: authz.OpAdmin, res: groupScoped},
	"GET /api/v1/groups/{group}/offsets/export":  {op: authz.OpRead, res: groupScoped},
	"POST /api/v1/groups/{group}/offsets/import": {op: authz.OpWrite, res: groupScoped},
	"GET /api/v1/logs":                           {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/throughput":                     {op: authz.OpDescribe, res: clusterScoped},
	"POST /api/v1/schemas/register":              {op: authz.OpWrite, res: clusterScoped},
	"GET /api/v1/schemas":                        {op: authz.OpDescribe, res: clusterScoped},
	"GET /api/v1/schemas/{subject}":              {op: authz.OpDescribe, res: clusterScoped},
	"GET /api/v1/mqtt":                           {op: authz.OpDescribe, res: clusterScoped},
	"GET /api/v1/integrations":                   {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/config":                         {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/acls":                           {op: authz.OpAdmin, res: clusterScoped},
	"PUT /api/v1/acls":                           {op: authz.OpAdmin, res: clusterScoped},
	"DELETE /api/v1/acls":                        {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/audit":                          {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/auth/delegated-tokens":          {op: authz.OpAdmin, res: clusterScoped},
	"POST /api/v1/auth/delegated-tokens":         {op: authz.OpAdmin, res: clusterScoped},
	"DELETE /api/v1/auth/delegated-tokens/{id}":  {op: authz.OpAdmin, res: clusterScoped},
	"GET /api/v1/health/overview":                {op: authz.OpDescribe, res: clusterScoped},
	"GET /api/v1/health/clusters":                {op: authz.OpDescribe, res: clusterScoped},
	// Prometheus scrapes with a session or a delegated token. The values are
	// cluster-wide (topic names, rates), so they need a cluster-scoped Describe
	// rather than a scoped topic grant; the read-only cluster token is refused,
	// because it exists to fetch the health report.
	"GET /metrics":                    {op: authz.OpDescribe, res: clusterScoped},
	"GET /api/v1/clusters":            {op: authz.OpAdmin, res: clusterScoped},
	"POST /api/v1/clusters":           {op: authz.OpAdmin, res: clusterScoped},
	"POST /api/v1/clusters/test":      {op: authz.OpAdmin, res: clusterScoped},
	"DELETE /api/v1/clusters/{name}":  {op: authz.OpAdmin, res: clusterScoped},
	"/api/v1/target/{name}/{path...}": {op: authz.OpAdmin, res: clusterScoped},
}

// clusterTokenRoutes may be called with the read-only cluster token: they
// report this broker's health and nothing else. Every other route (including
// /metrics) requires a real identity.
var clusterTokenRoutes = map[string]bool{
	"GET /api/v1/health/overview": true,
}

// apiPublicPaths are reachable without a session: the login endpoints, the
// session status probe the SPA needs to decide whether to show the login form,
// and nothing else.
func apiPublicPath(path string) bool {
	switch path {
	case "/api/v1/auth/login", "/api/v1/auth/logout", "/api/v1/auth/status":
		return true
	}
	return false
}

// lookupRule returns the rule for a matched route pattern.
func lookupRule(pattern string) (apiRule, bool) {
	if pattern == "" {
		return apiRule{}, false
	}
	// A pattern may carry a trailing slash form; normalise before lookup.
	if r, ok := apiRules[strings.TrimSuffix(pattern, "/")]; ok {
		return r, true
	}
	r, ok := apiRules[pattern]
	return r, ok
}

// patternPath strips the method qualifier ServeMux puts on a matched pattern
// ("GET /api/v1/topics/{topic}" -> "/api/v1/topics/{topic}").
func patternPath(pattern string) string {
	if i := strings.IndexByte(pattern, ' '); i >= 0 {
		return strings.TrimSpace(pattern[i+1:])
	}
	return pattern
}

// matchPattern extracts the path parameters of a ServeMux pattern from a
// request path. It exists because the authorization middleware runs outside the
// mux, where http.Request.PathValue is not populated yet.
func matchPattern(pattern, path string) (map[string]string, bool) {
	pp := splitSegments(pattern)
	ps := splitSegments(path)
	params := make(map[string]string, 2)
	for i, seg := range pp {
		if seg == "{path...}" {
			if i >= len(ps) {
				return nil, false
			}
			params["path"] = strings.Join(ps[i:], "/")
			return params, true
		}
		if i >= len(ps) {
			return nil, false
		}
		if name, ok := pathParamName(seg); ok {
			params[name] = ps[i]
			continue
		}
		if seg != ps[i] {
			return nil, false
		}
	}
	if len(pp) != len(ps) {
		return nil, false
	}
	return params, true
}

func pathParamName(seg string) (string, bool) {
	if len(seg) < 3 || seg[0] != '{' || seg[len(seg)-1] != '}' {
		return "", false
	}
	name := seg[1 : len(seg)-1]
	if strings.HasSuffix(name, "...") {
		return "", false
	}
	return name, true
}

func splitSegments(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// isAPIPath reports whether a request is subject to API authorization.
func isAPIPath(path string) bool {
	return strings.HasPrefix(path, "/api/") || path == "/metrics"
}
