package gateway

import (
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
