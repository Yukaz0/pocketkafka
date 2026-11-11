package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

type authzFixture struct {
	handler http.Handler
	server  *Server
	acl     *authz.Store
}

func newAuthzFixture(t *testing.T, rules []authz.Rule) *authzFixture {
	t.Helper()
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "table-test-signing-secret"
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{
		{Username: "reader", Password: "reader-password"},
		{Username: "topicAdmin", Password: "topic-admin-password"},
		{Username: "clusterAdmin", Password: "cluster-admin-password"},
		{Username: "groupAdmin", Password: "group-admin-password"},
	}
	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if _, err := store.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("secrets", 1); err != nil {
		t.Fatal(err)
	}
	os, err := coordinator.NewOffsetStore("inmemory", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)

	acl := authz.NewInMemory()
	for _, r := range rules {
		if err := acl.Upsert(r); err != nil {
			t.Fatal(err)
		}
	}
	s := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(acl)
	return &authzFixture{handler: s.Handler(), server: s, acl: acl}
}

func (f *authzFixture) session(t *testing.T, user, password string) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"username": user, "password": password})
	req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set(csrfHeader, "1")
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("login %s = %d", user, rr.Code)
	}
	return rr.Result().Cookies()[0]
}

func (f *authzFixture) do(t *testing.T, method, path string, cookie *http.Cookie, body string) (int, string) {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if method != http.MethodGet && method != http.MethodHead {
		req.Header.Set(csrfHeader, "1")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	return rr.Code, rr.Body.String()
}

func TestRouteAuthorizationTable(t *testing.T) {
	reader := authz.Rule{Principal: "reader", ResourceType: authz.ResourceTopic, ResourceName: "orders", Operations: []string{"Read"}}
	topicAdmin := authz.Rule{Principal: "topicAdmin", ResourceType: authz.ResourceTopic, ResourceName: "orders", Operations: []string{"Admin"}}
	clusterAdmin := authz.Rule{Principal: "clusterAdmin", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Admin"}}

	f := newAuthzFixture(t, []authz.Rule{reader, topicAdmin, clusterAdmin})

	readerCookie := f.session(t, "reader", "reader-password")
	topicAdminCookie := f.session(t, "topicAdmin", "topic-admin-password")
	clusterAdminCookie := f.session(t, "clusterAdmin", "cluster-admin-password")

	cases := []struct {
		name   string
		method string
		path   string
		body   string
		cookie *http.Cookie
		want   int
	}{
		{"read own topic", "GET", "/api/v1/topics/orders/messages", "", readerCookie, http.StatusOK},
		{"read another topic", "GET", "/api/v1/topics/secrets/messages", "", readerCookie, http.StatusForbidden},
		{"truncate own topic", "POST", "/api/v1/topics/orders/truncate", `{"partition":0,"offset":0}`, readerCookie, http.StatusForbidden},
		{"read the audit trail", "GET", "/api/v1/audit", "", readerCookie, http.StatusForbidden},
		{"read the broker log", "GET", "/api/v1/logs", "", readerCookie, http.StatusForbidden},
		{"list the acls", "GET", "/api/v1/acls", "", readerCookie, http.StatusForbidden},
		{"read the broker config", "GET", "/api/v1/config", "", readerCookie, http.StatusForbidden},
		{"delete a topic", "DELETE", "/api/v1/topics/orders", "", readerCookie, http.StatusForbidden},
		{"create a topic", "POST", "/api/v1/topics", `{"name":"newtopic","partitions":1}`, readerCookie, http.StatusForbidden},
		{"list topics is allowed for a scoped reader", "GET", "/api/v1/topics", "", readerCookie, http.StatusOK},

		{"truncate own topic as its admin", "POST", "/api/v1/topics/orders/truncate", `{"partition":0,"offset":0}`, topicAdminCookie, http.StatusOK},
		{"truncate another topic", "POST", "/api/v1/topics/secrets/truncate", `{"partition":0,"offset":0}`, topicAdminCookie, http.StatusForbidden},
		{"read message contents without Read", "GET", "/api/v1/topics/secrets/messages", "", topicAdminCookie, http.StatusForbidden},
		{"reach the cluster-wide log", "GET", "/api/v1/logs", "", topicAdminCookie, http.StatusForbidden},

		{"cluster admin lists acls", "GET", "/api/v1/acls", "", clusterAdminCookie, http.StatusOK},
		{"cluster admin reads the log", "GET", "/api/v1/logs", "", clusterAdminCookie, http.StatusOK},
		{"cluster admin manages monitored clusters", "GET", "/api/v1/clusters", "", clusterAdminCookie, http.StatusOK},
		{"cluster admin manages tokens", "GET", "/api/v1/auth/delegated-tokens", "", clusterAdminCookie, http.StatusOK},
		{"cluster admin sees the topic list", "GET", "/api/v1/topics", "", clusterAdminCookie, http.StatusOK},
		{"cluster admin cannot read message contents without a topic grant", "GET", "/api/v1/topics/orders/messages", "", clusterAdminCookie, http.StatusForbidden},
		{"topic admin lists the topics (payload filtered by the handler)", "GET", "/api/v1/topics", "", topicAdminCookie, http.StatusOK},
		{"cluster admin scrapes metrics", "GET", "/metrics", "", clusterAdminCookie, http.StatusOK},
		{"scoped reader cannot scrape cluster-wide metrics", "GET", "/metrics", "", readerCookie, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := f.do(t, tc.method, tc.path, tc.cookie, tc.body)
			if code != tc.want {
				t.Fatalf("%s %s = %d (want %d) body=%s", tc.method, tc.path, code, tc.want, body)
			}
		})
	}
}

func TestScopedPrincipalNeverSeesUngrantedTopics(t *testing.T) {
	f := newAuthzFixture(t, []authz.Rule{{
		Principal: "reader", ResourceType: authz.ResourceTopic, ResourceName: "orders", Operations: []string{"Read"},
	}})
	cookie := f.session(t, "reader", "reader-password")

	granted := "orders"
	var forbidden []string
	for _, name := range f.server.store.TopicNames() {
		if name != granted {
			forbidden = append(forbidden, name)
		}
	}
	if len(forbidden) == 0 {
		t.Fatal("the fixture has no ungranted topic, so the sweep cannot detect anything")
	}

	placeholders := []string{"{topic}", "{group}", "{subject}", "{id}", "{name}", "{path...}"}
	values := []string{granted, "g1", "s1", "tok1", "peer", "api/v1/health/overview"}
	exercised := 0
	for pattern := range apiRules {
		if !strings.HasPrefix(pattern, "GET ") {
			continue
		}
		path := strings.TrimPrefix(pattern, "GET ")
		for i, ph := range placeholders {
			path = strings.ReplaceAll(path, ph, values[i])
		}
		if strings.Contains(path, "{") {
			t.Fatalf("pattern %q has a placeholder this sweep does not know about", pattern)
		}
		code, body := f.do(t, "GET", path, cookie, "")
		if code != http.StatusOK {
			continue
		}
		exercised++
		for _, name := range forbidden {
			if strings.Contains(body, name) {
				t.Errorf("GET %s returned topic %q, which the principal may not see: %s", path, name, body)
			}
		}
	}
	if exercised == 0 {
		t.Fatal("no route answered 200 for the scoped principal; the sweep is vacuous")
	}
}

func TestTopicListIsFiltered(t *testing.T) {
	f := newAuthzFixture(t, []authz.Rule{{
		Principal: "reader", ResourceType: authz.ResourceTopic, ResourceName: "orders", Operations: []string{"Read"},
	}})
	cookie := f.session(t, "reader", "reader-password")
	code, body := f.do(t, "GET", "/api/v1/topics", cookie, "")
	if code != http.StatusOK {
		t.Fatalf("topics = %d body=%s", code, body)
	}
	if !strings.Contains(body, `"orders"`) {
		t.Fatalf("listing should include the granted topic: %s", body)
	}
	if strings.Contains(body, `"secrets"`) {
		t.Fatalf("listing leaked a topic the principal may not see: %s", body)
	}
}

func TestClusterTokenIsLimitedToHealth(t *testing.T) {
	const token = "cluster-token-0123456789"
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "table-test-signing-secret"
	cfg.Web.ClusterToken = token
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "reader", Password: "reader-password"}}

	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("orders", 1); err != nil {
		t.Fatal(err)
	}
	os, _ := coordinator.NewOffsetStore("inmemory", t.TempDir())
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)
	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).
		WithDataDir(t.TempDir()).
		WithClusterMonitoring(cfg).
		WithACLStore(authz.NewInMemory()).Handler()

	get := func(path string) int {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := get("/api/v1/health/overview"); code != http.StatusOK {
		t.Fatalf("health with the cluster token = %d, want 200", code)
	}
	for _, path := range []string{
		"/api/v1/topics",
		"/api/v1/topics/orders/messages",
		"/api/v1/audit",
		"/api/v1/logs",
		"/api/v1/acls",
		"/api/v1/config",
		"/metrics",
	} {
		if code := get(path); code != http.StatusForbidden {
			t.Fatalf("cluster token reached %s = %d, want 403", path, code)
		}
	}
}

func TestEveryRouteHasAuthorizationRule(t *testing.T) {
	f := newAuthzFixture(t, nil)
	s := f.server
	if len(s.registered) == 0 {
		s.routes()
	}
	if len(s.registered) == 0 {
		t.Fatal("no route patterns were recorded")
	}
	covered := 0
	for _, pattern := range s.registered {
		path := patternPath(pattern)
		if !isAPIPath(path) || apiPublicPath(path) {
			continue
		}
		if _, ok := lookupRule(pattern); !ok {
			t.Errorf("route %q has no authorization rule: every identity, including a cluster admin, would get 403", pattern)
			continue
		}
		covered++
	}
	if covered == 0 {
		t.Fatal("no API route was checked; the recorder is not wired")
	}
}

func TestEveryRegisteredRouteHasARule(t *testing.T) {
	f := newAuthzFixture(t, nil)
	mux := f.server.routes()
	placeholders := map[string]string{
		"topic": "orders", "group": "g1", "id": "tok1", "name": "peer", "subject": "s1", "path": "api/v1/health/overview",
	}
	for pattern := range apiRules {
		path := pattern
		if i := strings.IndexByte(path, ' '); i >= 0 {
			path = path[i+1:]
		}
		for name, value := range placeholders {
			path = strings.ReplaceAll(path, "{"+name+"}", value)
			path = strings.ReplaceAll(path, "{"+name+"...}", value)
		}
		if strings.Contains(path, "{") {
			t.Fatalf("pattern %q has an unfilled placeholder", pattern)
		}
		method := http.MethodGet
		if i := strings.IndexByte(pattern, ' '); i >= 0 {
			method = pattern[:i]
		}
		req := httptest.NewRequest(method, path, nil)
		_, matchedPattern := mux.Handler(req)
		if _, ok := lookupRule(matchedPattern); !ok {
			t.Fatalf("matched pattern %q has no authorization rule", matchedPattern)
		}
	}
}
