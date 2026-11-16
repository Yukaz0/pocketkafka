package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func TestRESTProxyPublishRoute(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewRESTProxy(store).Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/topics/rp/messages", "application/json", strings.NewReader(`{"value":{"x":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("POST publish status: got %d want 201", resp.StatusCode)
	}
}

// Frozen v1.0 REST contract (docs/STABILITY.md): field names and status codes.
func TestRESTProxyContractFrozen(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewRESTProxy(store).Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/topics/c/messages", "application/json",
		strings.NewReader(`{"key":"k","value":"v","headers":{"h":"1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	var pub map[string]any
	json.NewDecoder(resp.Body).Decode(&pub)
	resp.Body.Close()
	if resp.StatusCode != 201 || pub["topic"] != "c" || pub["partition"] != float64(0) || pub["offset"] != float64(0) {
		t.Fatalf("publish: status %d body %v", resp.StatusCode, pub)
	}

	resp, err = http.Get(srv.URL + "/topics/c/messages?offset=0&limit=10&partition=0")
	if err != nil {
		t.Fatal(err)
	}
	var recs []map[string]any
	json.NewDecoder(resp.Body).Decode(&recs)
	resp.Body.Close()
	if resp.StatusCode != 200 || len(recs) != 1 {
		t.Fatalf("consume: status %d records %v", resp.StatusCode, recs)
	}
	r := recs[0]
	hdrs, _ := r["headers"].(map[string]any)
	if r["topic"] != "c" || r["partition"] != float64(0) || r["offset"] != float64(0) ||
		r["key"] != "k" || r["value"] != "v" || hdrs["h"] != "1" {
		t.Fatalf("consume record shape changed: %v", r)
	}

	resp, err = http.Get(srv.URL + "/topics/nope/messages")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("unknown topic: status %d, want 404", resp.StatusCode)
	}
}
