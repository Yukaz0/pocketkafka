package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
)

func TestClusterTestDoesNotReuseStoredTokenForAnotherTarget(t *testing.T) {
	s, _ := newClusterTestServer(t, nil, nil)

	var seenAuth string
	peer := fakePeer(t, http.StatusOK, peerReport, &seenAuth)

	entry := ClusterEntry{Name: "prod", Kind: clusterKindPeer, URL: peer.URL}
	token := "stored-peer-token-0123456789"
	if err := s.clusters.upsert(entry, &token, nil, nil); err != nil {
		t.Fatal(err)
	}
	h := s.Handler()

	post := func(body string) (int, string) {
		req := httptest.NewRequest("POST", "/api/v1/clusters/test", strings.NewReader(body))
		req.Header.Set(csrfHeader, "1")
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code, rr.Body.String()
	}

	if code, body := post(`{"name":"prod","url":"` + peer.URL + `"}`); code != http.StatusOK {
		t.Fatalf("testing the stored target = %d body=%s", code, body)
	}
	if seenAuth != "Bearer "+token {
		t.Fatalf("stored token not forwarded to its own target: %q", seenAuth)
	}

	other := fakePeer(t, http.StatusOK, peerReport, nil)
	seenAuth = ""
	code, body := post(`{"name":"prod","url":"` + other.URL + `"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("testing a changed target = %d (want 400) body=%s", code, body)
	}
	if seenAuth != "" {
		t.Fatalf("a request reached the changed target carrying %q", seenAuth)
	}
	if strings.Contains(body, token) {
		t.Fatalf("response leaked the stored token: %s", body)
	}
}

func TestClusterTestDoesNotReuseStoredPasswordForAnotherBrokerList(t *testing.T) {
	s, _ := newClusterTestServer(t, nil, nil)
	password := "stored-sasl-password"
	entry := ClusterEntry{Name: "legacy", Kind: clusterKindKafka, Brokers: []string{"broker.internal:9092"}, SASLUser: "monitor"}
	if err := s.clusters.upsert(entry, nil, &password, nil); err != nil {
		t.Fatal(err)
	}
	h := s.Handler()

	req := httptest.NewRequest("POST", "/api/v1/clusters/test", strings.NewReader(
		`{"name":"legacy","kind":"kafka","brokers":["attacker.example:9092"],"saslUser":"monitor"}`))
	req.Header.Set(csrfHeader, "1")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("changed broker list = %d (want 400) body=%s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), password) {
		t.Fatalf("response leaked the stored password: %s", rr.Body.String())
	}
}

func TestUpsertClusterRejectsUnknownKind(t *testing.T) {
	s, _ := newClusterTestServer(t, nil, func(c *config.Config) {
		c.Web.SecretsKey = "a-strong-secrets-key"
	})
	h := s.Handler()
	req := httptest.NewRequest("POST", "/api/v1/clusters", strings.NewReader(`{"name":"x","kind":"redis","url":"http://h:1"}`))
	req.Header.Set(csrfHeader, "1")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unknown kind = %d (want 400)", rr.Code)
	}
}
