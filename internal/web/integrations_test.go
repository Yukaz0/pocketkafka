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
	"github.com/Yukaz0/pocketkafka/internal/gateway"
)

func testACL(t *testing.T, principals ...string) *authz.Store {
	t.Helper()
	acl := authz.NewInMemory()
	for _, principal := range principals {
		if principal == "" {
			continue
		}
		if err := acl.Upsert(authz.Rule{
			Principal:    principal,
			ResourceType: authz.ResourceCluster,
			ResourceName: "*",
			Operations:   []string{"Admin"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	return acl
}

func newIntegrationTestServer(t *testing.T, cfg config.Config) *Server {
	t.Helper()
	s, store, _ := newHealthTestServer(t)
	users := make([]string, 0, len(cfg.Security.Users))
	for _, u := range cfg.Security.Users {
		users = append(users, u.Username)
	}
	return s.WithDataDir(t.TempDir()).WithAuth(cfg).WithACLStore(testACL(t, users...)).WithConfig(cfg).
		WithGateway(cfg.Gateway.Listen).
		WithSchemaRegistryListen(cfg.SchemaRegistry.Listen).
		WithMQTT(gateway.NewMQTTBridge(store), cfg.MQTT.Listen, false)
}

func decodeObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("response is not a JSON object: %v (%s)", err, raw)
	}
	return out
}

func TestIntegrationsReportsConfiguredSurfaces(t *testing.T) {
	cfg := config.Default()
	cfg.Gateway.Listen = "10.0.0.5:8082"
	cfg.SchemaRegistry.Listen = "10.0.0.5:8081"
	cfg.MQTT.Listen = "10.0.0.5:1883"
	s := newIntegrationTestServer(t, cfg)

	code, body := getJSON(t, s.Handler(), "/api/v1/integrations", nil)
	if code != http.StatusOK {
		t.Fatalf("status = %d body=%s", code, body)
	}
	root := decodeObject(t, body)

	mqtt, ok := root["mqtt"].(map[string]any)
	if !ok {
		t.Fatalf("mqtt section missing: %s", body)
	}
	if mqtt["enabled"] != false {
		t.Fatalf("a bridge that never started must not report enabled: %v", mqtt)
	}
	if mqtt["listen"] != "10.0.0.5:1883" {
		t.Fatalf("mqtt.listen = %v, want the configured address", mqtt["listen"])
	}
	if mqtt["maxConnections"] != float64(gateway.MQTTMaxConnections) {
		t.Fatalf("mqtt.maxConnections = %v, want %d", mqtt["maxConnections"], gateway.MQTTMaxConnections)
	}
	if mqtt["topicPattern"] != gateway.MQTTTopicPattern || mqtt["kafkaTopicPattern"] != gateway.MQTTKafkaTopicPattern {
		t.Fatalf("mqtt topic mapping is not the documented one: %v", mqtt)
	}
	if mqtt["topicHeader"] != gateway.MQTTTopicHeader {
		t.Fatalf("mqtt.topicHeader = %v, want %q", mqtt["topicHeader"], gateway.MQTTTopicHeader)
	}

	rest, ok := root["restProxy"].(map[string]any)
	if !ok {
		t.Fatalf("restProxy section missing: %s", body)
	}
	if rest["enabled"] != true || rest["listen"] != "10.0.0.5:8082" {
		t.Fatalf("restProxy = %v, want the configured listener", rest)
	}
	if rest["auth"] != false {
		t.Fatalf("restProxy.auth = %v, want false with security disabled", rest["auth"])
	}
	endpoints, ok := rest["endpoints"].([]any)
	if !ok || len(endpoints) != 2 {
		t.Fatalf("restProxy.endpoints = %v, want the two proxy routes", rest["endpoints"])
	}

	sr, ok := root["schemaRegistry"].(map[string]any)
	if !ok {
		t.Fatalf("schemaRegistry section missing: %s", body)
	}
	if sr["listen"] != "10.0.0.5:8081" {
		t.Fatalf("schemaRegistry.listen = %v, want the configured address", sr["listen"])
	}
	if sr["enabled"] != false {
		t.Fatalf("schemaRegistry.enabled = %v, want false without a registry", sr["enabled"])
	}

	metrics, ok := root["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("metrics section missing: %s", body)
	}
	if metrics["path"] != "/metrics" || metrics["authRequired"] != false {
		t.Fatalf("metrics = %v, want /metrics with authRequired false", metrics)
	}
	if metrics["grafanaDashboard"] != "deploy/grafana/dashboard.json" {
		t.Fatalf("metrics.grafanaDashboard = %v", metrics["grafanaDashboard"])
	}
}

func TestMQTTStatusReportsFailedStart(t *testing.T) {
	cfg := config.Default()
	cfg.MQTT.Listen = "10.0.0.5:1883"
	s, store, _ := newHealthTestServer(t)
	bridge := gateway.NewMQTTBridge(store)

	for _, tc := range []struct {
		name    string
		running bool
		want    any
	}{
		{name: "bound", running: true, want: true},
		{name: "port taken", running: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := s.WithAuth(cfg).WithMQTT(bridge, cfg.MQTT.Listen, tc.running).Handler()
			code, body := getJSON(t, h, "/api/v1/mqtt", nil)
			if code != http.StatusOK {
				t.Fatalf("status = %d body=%s", code, body)
			}
			status := decodeObject(t, body)
			if status["enabled"] != tc.want {
				t.Fatalf("mqtt status enabled = %v, want %v", status["enabled"], tc.want)
			}
			if status["listen"] != cfg.MQTT.Listen {
				t.Fatalf("mqtt status listen = %v, want the configured address", status["listen"])
			}

			code, body = getJSON(t, h, "/api/v1/integrations", nil)
			if code != http.StatusOK {
				t.Fatalf("status = %d body=%s", code, body)
			}
			integrations := decodeObject(t, body)
			mqtt := integrations["mqtt"].(map[string]any)
			if mqtt["enabled"] != tc.want {
				t.Fatalf("integrations mqtt enabled = %v, want %v", mqtt["enabled"], tc.want)
			}
		})
	}
}

func TestConfigEndpointOmitsSecrets(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "auth-secret-value"
	cfg.Web.SecretsKey = "secrets-key-value"
	cfg.Web.ClusterToken = "cluster-token-value"
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "ops", Password: "operator-password"}}
	cfg.Storage.Tiered = config.Tiered{
		Enabled: true, Endpoint: "https://access:credential@minio.internal:9000",
		Bucket: "pocketkafka", AccessKey: "ACCESS-KEY-VALUE", SecretKey: "SECRET-KEY-VALUE",
		OffloadAfterHours: 24, CheckIntervalMs: 300000,
	}
	s := newIntegrationTestServer(t, cfg)
	h := s.Handler()

	if code, _ := getJSON(t, h, "/api/v1/config", nil); code != http.StatusUnauthorized {
		t.Fatalf("config without a session = %d, want 401", code)
	}
	login, _ := json.Marshal(map[string]string{"username": "ops", "password": "operator-password"})
	loginReq := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(login))
	loginReq.Header.Set(csrfHeader, "1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, loginReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("login = %d body=%s", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("login issued no cookie")
	}
	req := httptest.NewRequest("GET", "/api/v1/config", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("config with a session = %d body=%s", rec.Code, rec.Body.String())
	}
	raw := rec.Body.String()

	for _, secret := range []string{
		cfg.Web.AuthSecret, cfg.Web.SecretsKey, cfg.Web.ClusterToken,
		cfg.Security.Users[0].Password, cfg.Storage.Tiered.AccessKey, cfg.Storage.Tiered.SecretKey,
		"access:credential@",
	} {
		if strings.Contains(raw, secret) {
			t.Fatalf("config response leaks %q: %s", secret, raw)
		}
	}

	root := decodeObject(t, raw)
	keys := map[string]bool{}
	collectJSONKeys(root, keys)
	for _, forbidden := range []string{"password", "authSecret", "secretsKey", "clusterToken", "accessKey", "secretKey"} {
		if keys[forbidden] {
			t.Fatalf("config response exposes a %q field: %s", forbidden, raw)
		}
	}

	storage, ok := root["storage"].(map[string]any)
	if !ok {
		t.Fatalf("storage section missing: %s", raw)
	}
	tiered, ok := storage["tiered"].(map[string]any)
	if !ok {
		t.Fatalf("storage.tiered section missing: %s", raw)
	}
	if tiered["endpoint"] != "minio.internal:9000" {
		t.Fatalf("tiered endpoint = %v, want host:port without credentials", tiered["endpoint"])
	}
	if tiered["bucket"] != "pocketkafka" || tiered["offloadAfterHours"] != float64(24) {
		t.Fatalf("tiered settings are not reported: %v", tiered)
	}

	security, ok := root["security"].(map[string]any)
	if !ok {
		t.Fatalf("security section missing: %s", raw)
	}
	users, ok := security["users"].([]any)
	if !ok || len(users) != 1 || users[0] != "ops" {
		t.Fatalf("security.users = %v, want the usernames only", security["users"])
	}

	listeners, ok := root["listeners"].(map[string]any)
	if !ok || listeners["internal"] != config.Default().Listeners["internal"] {
		t.Fatalf("listeners = %v, want the effective defaults", root["listeners"])
	}
}

func TestTargetRouteAllowlistsCoverTheNewReadSurfaces(t *testing.T) {
	for _, path := range []string{"/api/v1/integrations", "/api/v1/config"} {
		if !peerTargetRouteAllowed(http.MethodGet, path) {
			t.Fatalf("peer proxy must allow GET %s", path)
		}
		if peerTargetRouteAllowed(http.MethodPost, path) {
			t.Fatalf("peer proxy must not allow POST %s", path)
		}
		if kafkaReadEndpoint(path) {
			t.Fatalf("Kafka target must not claim to serve %s", path)
		}
	}
}

func TestMQTTPublishAndInfoEndpoints(t *testing.T) {
	cfg := config.Default()
	s, store, _ := newHealthTestServer(t)
	bridge := gateway.NewMQTTBridge(store)
	h := s.WithAuth(cfg).WithMQTT(bridge, "127.0.0.1:1883", true).Handler()

	// 1. Info endpoint reports mqttEnabled: true
	code, body := getJSON(t, h, "/api/v1/info", nil)
	if code != http.StatusOK {
		t.Fatalf("info status = %d, want 200", code)
	}
	info := decodeObject(t, body)
	if info["mqttEnabled"] != true {
		t.Fatalf("expected mqttEnabled true, got %v", info["mqttEnabled"])
	}

	// 2. MQTT status includes clients array
	code, body = getJSON(t, h, "/api/v1/mqtt", nil)
	if code != http.StatusOK {
		t.Fatalf("mqtt status = %d, want 200", code)
	}
	status := decodeObject(t, body)
	clients, ok := status["clients"].([]any)
	if !ok {
		t.Fatalf("expected clients array, got %T (%v)", status["clients"], status["clients"])
	}
	if len(clients) != 0 {
		t.Fatalf("expected 0 clients, got %d", len(clients))
	}

	// 3. Publish endpoint validation: invalid QoS
	badQoSReq := httptest.NewRequest("POST", "/api/v1/mqtt/publish", strings.NewReader(`{"topic":"sensors/temp","payload":"20","qos":5}`))
	badQoSReq.Header.Set("X-PocketKafka-Request", "1")
	badQoSRec := httptest.NewRecorder()
	h.ServeHTTP(badQoSRec, badQoSReq)
	if badQoSRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad QoS, got %d body=%s", badQoSRec.Code, badQoSRec.Body.String())
	}

	// 4. Publish endpoint validation: empty topic
	badTopicReq := httptest.NewRequest("POST", "/api/v1/mqtt/publish", strings.NewReader(`{"topic":"","payload":"20","qos":0}`))
	badTopicReq.Header.Set("X-PocketKafka-Request", "1")
	badTopicRec := httptest.NewRecorder()
	h.ServeHTTP(badTopicRec, badTopicReq)
	if badTopicRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty topic, got %d", badTopicRec.Code)
	}

	// 5. Valid Publish
	pubReq := httptest.NewRequest("POST", "/api/v1/mqtt/publish", strings.NewReader(`{"topic":"sensors/livingroom/temp","payload":"21.5","qos":0}`))
	pubReq.Header.Set("X-PocketKafka-Request", "1")
	pubRec := httptest.NewRecorder()
	h.ServeHTTP(pubRec, pubReq)
	if pubRec.Code != http.StatusCreated {
		t.Fatalf("expected 201 for publish, got %d: %s", pubRec.Code, pubRec.Body.String())
	}
	res := decodeObject(t, pubRec.Body.String())
	if res["topic"] != "sensors/livingroom/temp" {
		t.Fatalf("expected topic %q, got %q", "sensors/livingroom/temp", res["topic"])
	}
	if res["kafkaTopic"] != "mqtt-sensors" {
		t.Fatalf("expected kafkaTopic %q, got %q", "mqtt-sensors", res["kafkaTopic"])
	}

	// Verify record in Kafka store
	part := store.GetPartition("mqtt-sensors", 0)
	if part == nil {
		t.Fatal("expected partition mqtt-sensors:0 to exist")
	}
	if part.HighWatermark() != 1 {
		t.Fatalf("expected 1 record in partition, high watermark = %d", part.HighWatermark())
	}

	// 6. When MQTT bridge is not running, Publish returns 503
	hDisabled := s.WithAuth(cfg).WithMQTT(bridge, "127.0.0.1:1883", false).Handler()
	disReq := httptest.NewRequest("POST", "/api/v1/mqtt/publish", strings.NewReader(`{"topic":"sensors/x","payload":"1"}`))
	disReq.Header.Set("X-PocketKafka-Request", "1")
	disRec := httptest.NewRecorder()
	hDisabled.ServeHTTP(disRec, disReq)
	if disRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when mqtt disabled, got %d", disRec.Code)
	}
}

func collectJSONKeys(node any, keys map[string]bool) {
	switch typed := node.(type) {
	case map[string]any:
		for key, child := range typed {
			keys[key] = true
			collectJSONKeys(child, keys)
		}
	case []any:
		for _, child := range typed {
			collectJSONKeys(child, keys)
		}
	}
}
