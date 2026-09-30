package web

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
)

// peerReport is a canned health report used to stand in for a remote broker.
const peerReport = `{
  "generatedAtMs": 1790000000000,
  "sampling": {"intervalMs": 5000, "samples": 40, "windowSeconds": 60, "warmup": false, "rateReliable": true},
  "cluster": {"clusterId": "peer-cluster", "brokerId": 7, "version": "1.0.0", "status": "degraded",
              "topics": 7, "partitions": 9, "groups": 2, "totalMessages": 1234, "lag": 55,
              "maxPartitionLag": 55, "messagesPerSec": 3.5, "bytesPerSec": 700.25, "diskUsagePct": 12.5},
  "topics": [],
  "attention": [{"name": "orders", "status": "degraded", "reasons": [{"code": "group_stalled"}]}],
  "groups": [],
  "history": []
}`

// fakePeer stands up a broker-like HTTP endpoint. It records the Authorization
// header it received so forwarding can be asserted.
func fakePeer(t *testing.T, status int, body string, seenAuth *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/health/overview" {
			http.NotFound(w, r)
			return
		}
		if seenAuth != nil {
			*seenAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newClusterTestServer(t *testing.T, seed []config.WebCluster, mutate func(*config.Config)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	s, _, _ := newHealthTestServer(t)
	cfg := config.Default()
	cfg.Web.Clusters = seed
	if mutate != nil {
		mutate(&cfg)
	}
	s.WithDataDir(dir).WithAuth(cfg).WithClusterMonitoring(cfg)
	return s, dir
}

func getJSON(t *testing.T, h http.Handler, path string, headers map[string]string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code, rr.Body.String()
}

// TestClusterRegistryPersistence covers the registry lifecycle: the config is a
// seed, the file is the source of truth afterwards, and a token is stored but
// never listed.
func TestClusterRegistryPersistence(t *testing.T) {
	s, dir := newClusterTestServer(t, []config.WebCluster{{Name: "staging", URL: "http://10.0.0.26:8080"}}, nil)

	list := s.clusters.List()
	if len(list) != 1 || list[0].Name != "staging" {
		t.Fatalf("seed not loaded: %+v", list)
	}
	if _, err := os.Stat(filepath.Join(dir, clustersFileName)); err != nil {
		t.Fatalf("seed was not persisted: %v", err)
	}

	token := "peer-token-0123456789"
	if err := s.clusters.Upsert(ClusterEntry{Name: "prod", URL: "https://prod.example:8080"}, &token); err != nil {
		t.Fatal(err)
	}
	if !s.clusters.HasToken("prod") || s.clusters.Token("prod") != token {
		t.Fatal("token was not stored")
	}
	if err := s.clusters.Remove("staging"); err != nil {
		t.Fatal(err)
	}

	// Reload from disk: the file wins, so a cluster removed from the UI does
	// not come back from the config seed.
	reloaded := newClusterStore(dir, []config.WebCluster{{Name: "staging", URL: "http://10.0.0.26:8080"}})
	after := reloaded.List()
	if len(after) != 1 || after[0].Name != "prod" {
		t.Fatalf("registry after reload = %+v, want only prod", after)
	}
	if reloaded.Token("prod") != token {
		t.Fatal("token did not survive the reload")
	}
	raw, err := os.ReadFile(filepath.Join(dir, clusterTokensFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), token) == false {
		t.Fatal("token file should hold the token")
	}
	if info, err := os.Stat(filepath.Join(dir, clusterTokensFile)); err == nil && info.Mode().Perm() != 0o600 {
		t.Fatalf("token file mode = %v, want 0600", info.Mode().Perm())
	}
}

// TestClusterRegistryValidation keeps malformed entries out of both the config
// and the UI path.
func TestClusterRegistryValidation(t *testing.T) {
	badURLs := []string{"", "10.0.0.26:8080", "file:///etc/passwd", "http://user:pass@host:8080", "ftp://host"}
	for _, u := range badURLs {
		if err := config.ValidateClusterURL(u); err == nil {
			t.Fatalf("ValidateClusterURL(%q) accepted a bad URL", u)
		}
	}
	for _, u := range []string{"http://10.0.0.26:8080", "https://prod.example.com"} {
		if err := config.ValidateClusterURL(u); err != nil {
			t.Fatalf("ValidateClusterURL(%q) rejected a good URL: %v", u, err)
		}
	}
	for _, n := range []string{"", "  ", strings.Repeat("x", 65), "bad\nname"} {
		if err := config.ValidateClusterName(n); err == nil {
			t.Fatalf("ValidateClusterName(%q) accepted a bad name", n)
		}
	}

	s, _ := newClusterTestServer(t, nil, nil)
	if err := s.clusters.Upsert(ClusterEntry{Name: "x", URL: "file:///etc/passwd"}, nil); err == nil {
		t.Fatal("registry accepted a non-http URL")
	}
	if err := s.clusters.Upsert(ClusterEntry{Name: "", URL: "http://host"}, nil); err == nil {
		t.Fatal("registry accepted an empty name")
	}
}

// TestClustersOverviewFanout is the happy path: one screen showing this broker
// plus every registered peer.
func TestClustersOverviewFanout(t *testing.T) {
	peer := fakePeer(t, http.StatusOK, peerReport, nil)
	s, _ := newClusterTestServer(t, []config.WebCluster{{Name: "peer", URL: peer.URL}}, nil)

	code, body := getJSON(t, s.Handler(), "/api/v1/health/clusters?window=60", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s", code, body)
	}
	var got struct {
		Clusters []struct {
			Name       string  `json:"name"`
			Self       bool    `json:"self"`
			OK         bool    `json:"ok"`
			Status     string  `json:"status"`
			Topics     int     `json:"topics"`
			Lag        int64   `json:"lag"`
			Attention  int     `json:"attention"`
			ClusterID  string  `json:"clusterId"`
			Disk       float64 `json:"diskUsagePct"`
			MsgPerSec  float64 `json:"messagesPerSec"`
			RateOK     bool    `json:"rateReliable"`
			Error      string  `json:"error"`
			HasToken   bool    `json:"hasToken"`
			SourceNote string  `json:"-"`
		} `json:"clusters"`
		Worst       string `json:"worst"`
		Unreachable int    `json:"unreachable"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("bad JSON: %v (%s)", err, body)
	}
	if len(got.Clusters) != 2 {
		t.Fatalf("clusters = %d, want 2 (self + peer)", len(got.Clusters))
	}
	if !got.Clusters[0].Self || got.Clusters[0].Name != "local" {
		t.Fatalf("first row should be this broker: %+v", got.Clusters[0])
	}
	p := got.Clusters[1]
	if !p.OK || p.Name != "peer" || p.Status != "degraded" || p.Topics != 7 || p.Lag != 55 || p.Attention != 1 {
		t.Fatalf("peer row not summarised from its report: %+v", p)
	}
	if !p.RateOK || p.ClusterID != "peer-cluster" || p.MsgPerSec != 3.5 || p.Disk != 12.5 {
		t.Fatalf("peer row missing fields: %+v", p)
	}
	if got.Unreachable != 0 {
		t.Fatalf("unreachable = %d, want 0", got.Unreachable)
	}
}

// TestClustersOverviewSurvivesDeadPeer: monitoring must not fail because the
// thing it monitors is down.
func TestClustersOverviewSurvivesDeadPeer(t *testing.T) {
	dead := fakePeer(t, http.StatusInternalServerError, "boom", nil)
	s, _ := newClusterTestServer(t, []config.WebCluster{{Name: "dead", URL: dead.URL}}, nil)

	code, body := getJSON(t, s.Handler(), "/api/v1/health/clusters", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200 even with a dead peer (body %s)", code, body)
	}
	var got struct {
		Clusters []struct {
			Name  string `json:"name"`
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		} `json:"clusters"`
		Unreachable int `json:"unreachable"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.Unreachable != 1 {
		t.Fatalf("unreachable = %d, want 1", got.Unreachable)
	}
	if got.Clusters[1].OK || !strings.Contains(got.Clusters[1].Error, "500") {
		t.Fatalf("dead peer row = %+v, want ok=false with the HTTP error", got.Clusters[1])
	}

	// A peer that answers garbage is reported, not crashed on.
	liar := fakePeer(t, http.StatusOK, "not json", nil)
	s2, _ := newClusterTestServer(t, []config.WebCluster{{Name: "liar", URL: liar.URL}}, nil)
	_, body2 := getJSON(t, s2.Handler(), "/api/v1/health/clusters", nil)
	if !strings.Contains(body2, "invalid JSON") {
		t.Fatalf("peer returning garbage should be reported: %s", body2)
	}
}

// TestRemoteOverviewPassthrough: a remote report is proxied untouched, and the
// stored token is forwarded as a bearer credential.
func TestRemoteOverviewPassthrough(t *testing.T) {
	var seen string
	peer := fakePeer(t, http.StatusOK, peerReport, &seen)
	s, _ := newClusterTestServer(t, nil, nil)
	if err := s.clusters.Upsert(ClusterEntry{Name: "peer", URL: peer.URL}, strPtr("peer-token-0123456789")); err != nil {
		t.Fatal(err)
	}

	h := s.Handler()
	code, body := getJSON(t, h, "/api/v1/health/overview?cluster=peer&window=60", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body %s", code, body)
	}
	if !strings.Contains(body, "peer-cluster") {
		t.Fatalf("passthrough body is not the peer report: %s", body)
	}
	if seen != "Bearer peer-token-0123456789" {
		t.Fatalf("peer saw auth %q, want the stored bearer token", seen)
	}

	if code, _ := getJSON(t, h, "/api/v1/health/overview?cluster=nope", nil); code != http.StatusNotFound {
		t.Fatalf("unknown cluster status = %d, want 404", code)
	}
	// local is never proxied: it is served by this broker.
	if code, body := getJSON(t, h, "/api/v1/health/overview?cluster=local", nil); code != http.StatusOK || !strings.Contains(body, "clusterId") {
		t.Fatalf("local cluster should be served locally: %d %s", code, body)
	}
}

// TestClusterTokenNeverExposed: the token is write-only in the API surface.
func TestClusterTokenNeverExposed(t *testing.T) {
	s, _ := newClusterTestServer(t, nil, nil)
	h := s.Handler()

	payload := `{"name":"peer","url":"http://10.0.0.9:8080","token":"peer-token-0123456789"}`
	req := httptest.NewRequest("POST", "/api/v1/clusters", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("upsert status = %d, body %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "peer-token-0123456789") {
		t.Fatal("upsert response leaked the token")
	}

	code, body := getJSON(t, h, "/api/v1/clusters", nil)
	if code != http.StatusOK {
		t.Fatalf("list status = %d", code)
	}
	if strings.Contains(body, "peer-token-0123456789") || strings.Contains(body, `"token"`) {
		t.Fatalf("listing leaked the token: %s", body)
	}
	if !strings.Contains(body, `"hasToken": true`) && !strings.Contains(body, `"hasToken":true`) {
		t.Fatalf("listing should report hasToken: %s", body)
	}

	// Delete removes both the entry and its token.
	req = httptest.NewRequest("DELETE", "/api/v1/clusters/peer", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("delete status = %d", rr.Code)
	}
	if s.clusters.HasToken("peer") || len(s.clusters.List()) != 0 {
		t.Fatal("delete left the cluster or its token behind")
	}
}

// TestClusterBearerAuth covers the accept side: an aggregating peer presents a
// bearer token instead of a browser session.
func TestClusterBearerAuth(t *testing.T) {
	const token = "aggregator-token-0123456789"
	s, _ := newClusterTestServer(t, nil, func(c *config.Config) {
		c.Web.Auth = true
		c.Web.AuthSecret = "test-secret"
		c.Web.ClusterToken = token
		c.Security.Enabled = true
		c.Security.Users = []config.SecurityUser{{Username: "app", Password: "changeme"}}
	})
	h := s.Handler()

	if code, _ := getJSON(t, h, "/api/v1/health/overview", nil); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", code)
	}
	if code, _ := getJSON(t, h, "/api/v1/health/overview", map[string]string{"Authorization": "Bearer wrong-token-000000"}); code != http.StatusUnauthorized {
		t.Fatalf("wrong bearer status = %d, want 401", code)
	}
	code, body := getJSON(t, h, "/api/v1/health/overview", map[string]string{"Authorization": "Bearer " + token})
	if code != http.StatusOK || !strings.Contains(body, "clusterId") {
		t.Fatalf("valid bearer status = %d, body %s", code, body)
	}
	// A bearer credential must not silently become a mutation grant.
	req := httptest.NewRequest("POST", "/api/v1/clusters", strings.NewReader(`{"name":"x","url":"http://h:1"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("bearer mutation status = %d, want 403 (ACL still applies)", rr.Code)
	}
}

// TestClusterFanoutCacheIsUsed keeps the UI poll from hammering peers.
func TestClusterFanoutCacheIsUsed(t *testing.T) {
	var hits int
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, peerReport)
	}))
	t.Cleanup(peer.Close)
	s, _ := newClusterTestServer(t, []config.WebCluster{{Name: "peer", URL: peer.URL}}, nil)
	h := s.Handler()

	for i := 0; i < 3; i++ {
		if code, _ := getJSON(t, h, "/api/v1/health/clusters", nil); code != http.StatusOK {
			t.Fatalf("status = %d", code)
		}
	}
	if hits != 1 {
		t.Fatalf("peer was fetched %d times, want 1 (cache should absorb the poll)", hits)
	}
}

func strPtr(s string) *string { return &s }
