package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/gateway"
)

func TestMetricsExposesBrokerOwnedGauges(t *testing.T) {
	s, store, _ := newHealthTestServer(t)
	bridge := gateway.NewMQTTBridge(store)
	h := s.WithMQTT(bridge, "127.0.0.1:1883", true).Handler()

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("metrics = %d", rr.Code)
	}
	body := rr.Body.String()

	for _, want := range []string{
		"# TYPE pocketkafka_disk_usage_ratio gauge",
		"pocketkafka_disk_usage_ratio ",
		"# TYPE pocketkafka_mqtt_connections gauge",
		"pocketkafka_mqtt_connections 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body is missing %q\n%s", want, body)
		}
	}

	h2 := s.WithMQTT(nil, "127.0.0.1:1883", false).Handler()
	rr = httptest.NewRecorder()
	h2.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rr.Body.String(), "pocketkafka_mqtt_connections 0") {
		t.Fatalf("mqtt gauge vanished with a nil bridge:\n%s", rr.Body.String())
	}

	s.metrics.RegisterGauge("pocketkafka_mqtt_connections", "duplicate", func() float64 { return 7 })
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body = rr.Body.String()
	if got := strings.Count(body, "# TYPE pocketkafka_mqtt_connections"); got != 1 {
		t.Fatalf("duplicate TYPE lines for one family: %d\n%s", got, body)
	}
	if !strings.Contains(body, "pocketkafka_mqtt_connections 7") {
		t.Fatalf("a later registration must still emit its sample:\n%s", body)
	}
}
