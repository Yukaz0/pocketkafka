package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/fleet"
)

type AgentState struct {
	AgentID  string `json:"agent_id"`
	ServerID string `json:"server_id"`
	Secret   string `json:"secret"`
}

func main() {
	var (
		serverFlag   = flag.String("server", getEnv("FLEET_SERVER_URL", "https://127.0.0.1:18090"), "fleet control server URL")
		tokenFlag    = flag.String("token", getEnv("FLEET_ENROLL_TOKEN", ""), "single-use enrollment token")
		stateFlag    = flag.String("state", getEnv("FLEET_STATE_PATH", "fleet-agent.json"), "path to agent state file")
		dockerFlag   = flag.String("docker-socket", getEnv("FLEET_DOCKER_SOCKET", "/var/run/docker.sock"), "path to docker socket")
		insecureFlag = flag.Bool("insecure", getEnv("FLEET_INSECURE", "") == "true" || getEnv("FLEET_INSECURE", "") == "1", "skip TLS verification")
		caCertFlag   = flag.String("ca-cert", getEnv("FLEET_CA_CERT", ""), "path to CA certificate PEM")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	tlsConfig := &tls.Config{
		InsecureSkipVerify: *insecureFlag,
	}
	if *caCertFlag != "" {
		caData, err := os.ReadFile(*caCertFlag)
		if err != nil {
			log.Fatalf("failed reading ca cert: %v", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caData)
		tlsConfig.RootCAs = pool
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: 30 * time.Second,
	}

	state, err := loadOrEnroll(ctx, httpClient, *serverFlag, *tokenFlag, *stateFlag)
	if err != nil {
		log.Fatalf("fleet-agent initialization failed: %v", err)
	}

	log.Printf("fleet-agent active: agent_id=%s server_id=%s", state.AgentID, state.ServerID)

	dockerClient := fleet.NewSocketDockerClient(*dockerFlag)
	if err := dockerClient.Ping(ctx); err != nil {
		log.Printf("warning: docker ping failed: %v", err)
	}

	transport := NewHTTPStreamTransport(httpClient, *serverFlag, state.AgentID, state.Secret)
	defer transport.Close()

	runner := fleet.NewAgentRunner(fleet.AgentRunnerConfig{
		AgentID:           state.AgentID,
		ServerID:          state.ServerID,
		Secret:            state.Secret,
		HeartbeatInterval: 10 * time.Second,
		Docker:            dockerClient,
		Transport:         transport,
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		log.Println("fleet-agent stopping gracefully")
	case err := <-errCh:
		if err != nil && ctx.Err() == nil {
			log.Printf("fleet-agent error: %v", err)
		}
	}
}

func loadOrEnroll(ctx context.Context, client *http.Client, serverURL, token, statePath string) (*AgentState, error) {
	if data, err := os.ReadFile(statePath); err == nil {
		var s AgentState
		if err := json.Unmarshal(data, &s); err == nil && s.AgentID != "" && s.Secret != "" {
			return &s, nil
		}
	}

	if token == "" {
		return nil, fmt.Errorf("state file %q not found and no enrollment token provided", statePath)
	}

	reqBody, _ := json.Marshal(map[string]string{"token": token})
	target := strings.TrimRight(serverURL, "/") + "/api/v1/fleet/enroll"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enroll request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("enroll failed (%d): %s", resp.StatusCode, string(b))
	}

	var enrolled struct {
		AgentID   string `json:"agent_id"`
		ServerID  string `json:"server_id"`
		RawSecret string `json:"raw_secret"`
		Secret    string `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&enrolled); err != nil {
		return nil, fmt.Errorf("decode enroll response: %w", err)
	}

	sec := enrolled.RawSecret
	if sec == "" {
		sec = enrolled.Secret
	}
	if sec == "" {
		return nil, fmt.Errorf("enroll response missing secret")
	}

	state := &AgentState{
		AgentID:  enrolled.AgentID,
		ServerID: enrolled.ServerID,
		Secret:   sec,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(statePath), 0700); err != nil && filepath.Dir(statePath) != "." {
		return nil, err
	}
	if err := os.WriteFile(statePath, data, 0600); err != nil {
		return nil, fmt.Errorf("save state failed: %w", err)
	}

	return state, nil
}

type HTTPStreamTransport struct {
	client    *http.Client
	serverURL string
	agentID   string
	secret    string
	inbound   chan *fleet.Envelope
	closeCh   chan struct{}
}

func NewHTTPStreamTransport(client *http.Client, serverURL, agentID, secret string) *HTTPStreamTransport {
	return &HTTPStreamTransport{
		client:    client,
		serverURL: strings.TrimRight(serverURL, "/"),
		agentID:   agentID,
		secret:    secret,
		inbound:   make(chan *fleet.Envelope, 32),
		closeCh:   make(chan struct{}),
	}
}

func (t *HTTPStreamTransport) Send(env *fleet.Envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	target := t.serverURL + "/api/v1/fleet/agent/envelope"
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("server rejected envelope (%d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (t *HTTPStreamTransport) Receive(ctx context.Context) (*fleet.Envelope, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closeCh:
		return nil, fmt.Errorf("transport closed")
	case env := <-t.inbound:
		return env, nil
	}
}

func (t *HTTPStreamTransport) Close() error {
	select {
	case <-t.closeCh:
	default:
		close(t.closeCh)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
