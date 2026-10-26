package fleet

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ContainerSummary struct {
	ID       string            `json:"id"`
	Names    []string          `json:"names"`
	Image    string            `json:"image"`
	Command  string            `json:"command"`
	Created  int64             `json:"created"`
	State    string            `json:"state"`
	Status   string            `json:"status"`
	Labels   map[string]string `json:"labels,omitempty"`
	ServerID string            `json:"server_id,omitempty"`
}

type ContainerInspect struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Image   string            `json:"image"`
	Created string            `json:"created"`
	State   ContainerState    `json:"state"`
	Config  ContainerConfig   `json:"config"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type ContainerState struct {
	Status     string `json:"status"`
	Running    bool   `json:"running"`
	Paused     bool   `json:"paused"`
	Restarting bool   `json:"restarting"`
	ExitCode   int    `json:"exit_code"`
	Error      string `json:"error"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
}

type ContainerConfig struct {
	Image  string            `json:"image"`
	Labels map[string]string `json:"labels,omitempty"`
	Env    []string          `json:"env,omitempty"`
}

type DockerClient interface {
	List(ctx context.Context) ([]ContainerSummary, error)
	Inspect(ctx context.Context, idOrName string) (*ContainerInspect, error)
	Start(ctx context.Context, idOrName string) error
	Stop(ctx context.Context, idOrName string, timeoutSeconds int) error
	Restart(ctx context.Context, idOrName string, timeoutSeconds int) error
	Logs(ctx context.Context, idOrName string, tailLines int) (string, error)
	Ping(ctx context.Context) error
}

type FakeDocker struct {
	mu         sync.RWMutex
	containers map[string]*ContainerInspect
	logs       map[string]string
	calls      []string
	pingErr    error
}

func NewFakeDocker() *FakeDocker {
	return &FakeDocker{
		containers: make(map[string]*ContainerInspect),
		logs:       make(map[string]string),
		calls:      make([]string, 0),
	}
}

func (f *FakeDocker) SetPingError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingErr = err
}

func (f *FakeDocker) SetLogs(idOrName, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[idOrName] = content
}

func (f *FakeDocker) AddContainer(c *ContainerInspect) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers[c.ID] = c
	if c.Name != "" {
		trimmed := strings.TrimPrefix(c.Name, "/")
		f.containers[trimmed] = c
	}
}

func (f *FakeDocker) Calls() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := make([]string, len(f.calls))
	copy(cp, f.calls)
	return cp
}

func (f *FakeDocker) Ping(ctx context.Context) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pingErr
}

func (f *FakeDocker) List(ctx context.Context) ([]ContainerSummary, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	seen := make(map[string]bool)
	var list []ContainerSummary
	for _, c := range f.containers {
		if seen[c.ID] {
			continue
		}
		seen[c.ID] = true
		list = append(list, ContainerSummary{
			ID:     c.ID,
			Names:  []string{c.Name},
			Image:  c.Image,
			State:  c.State.Status,
			Status: c.State.Status,
			Labels: c.Labels,
		})
	}
	return list, nil
}

func (f *FakeDocker) Inspect(ctx context.Context, idOrName string) (*ContainerInspect, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	c, ok := f.containers[idOrName]
	if !ok {
		c, ok = f.containers[strings.TrimPrefix(idOrName, "/")]
	}
	if !ok {
		return nil, fmt.Errorf("container %q not found: %w", idOrName, ErrNotFound)
	}
	cp := *c
	return &cp, nil
}

func (f *FakeDocker) Start(ctx context.Context, idOrName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "start:"+idOrName)

	c, ok := f.containers[idOrName]
	if !ok {
		c, ok = f.containers[strings.TrimPrefix(idOrName, "/")]
	}
	if !ok {
		return fmt.Errorf("container %q not found: %w", idOrName, ErrNotFound)
	}
	c.State.Running = true
	c.State.Status = "running"
	return nil
}

func (f *FakeDocker) Stop(ctx context.Context, idOrName string, timeoutSeconds int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("stop:%s:%d", idOrName, timeoutSeconds))

	c, ok := f.containers[idOrName]
	if !ok {
		c, ok = f.containers[strings.TrimPrefix(idOrName, "/")]
	}
	if !ok {
		return fmt.Errorf("container %q not found: %w", idOrName, ErrNotFound)
	}
	c.State.Running = false
	c.State.Status = "exited"
	return nil
}

func (f *FakeDocker) Restart(ctx context.Context, idOrName string, timeoutSeconds int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("restart:%s:%d", idOrName, timeoutSeconds))

	c, ok := f.containers[idOrName]
	if !ok {
		c, ok = f.containers[strings.TrimPrefix(idOrName, "/")]
	}
	if !ok {
		return fmt.Errorf("container %q not found: %w", idOrName, ErrNotFound)
	}
	c.State.Running = true
	c.State.Status = "running"
	return nil
}

func (f *FakeDocker) Logs(ctx context.Context, idOrName string, tailLines int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("logs:%s:%d", idOrName, tailLines))

	_, ok := f.containers[idOrName]
	if !ok {
		_, ok = f.containers[strings.TrimPrefix(idOrName, "/")]
	}
	if !ok {
		return "", fmt.Errorf("container %q not found: %w", idOrName, ErrNotFound)
	}

	if l, ok := f.logs[idOrName]; ok {
		return l, nil
	}
	if l, ok := f.logs[strings.TrimPrefix(idOrName, "/")]; ok {
		return l, nil
	}
	return "mock container logs", nil
}

type SocketDockerClient struct {
	client     *http.Client
	baseURL    string
	socketPath string
}

func NewSocketDockerClient(socketPath string) *SocketDockerClient {
	if socketPath == "" {
		socketPath = "/var/run/docker.sock"
	}

	var transport *http.Transport
	if strings.HasPrefix(socketPath, "tcp://") || strings.HasPrefix(socketPath, "http://") {
		clean := strings.TrimPrefix(strings.TrimPrefix(socketPath, "tcp://"), "http://")
		transport = &http.Transport{}
		return &SocketDockerClient{
			client:  &http.Client{Transport: transport, Timeout: 30 * time.Second},
			baseURL: "http://" + clean,
		}
	}

	transport = &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	return &SocketDockerClient{
		client:     &http.Client{Transport: transport, Timeout: 30 * time.Second},
		baseURL:    "http://localhost",
		socketPath: socketPath,
	}
}

func (c *SocketDockerClient) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker ping failed: status %d", resp.StatusCode)
	}
	return nil
}

func (c *SocketDockerClient) List(ctx context.Context) ([]ContainerSummary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/containers/json?all=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("docker list failed (%d): %s", resp.StatusCode, string(b))
	}

	var raw []struct {
		ID      string            `json:"Id"`
		Names   []string          `json:"Names"`
		Image   string            `json:"Image"`
		Command string            `json:"Command"`
		Created int64             `json:"Created"`
		State   string            `json:"State"`
		Status  string            `json:"Status"`
		Labels  map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	out := make([]ContainerSummary, len(raw))
	for i, r := range raw {
		out[i] = ContainerSummary{
			ID:      r.ID,
			Names:   r.Names,
			Image:   r.Image,
			Command: r.Command,
			Created: r.Created,
			State:   r.State,
			Status:  r.Status,
			Labels:  r.Labels,
		}
	}
	return out, nil
}

func (c *SocketDockerClient) Inspect(ctx context.Context, idOrName string) (*ContainerInspect, error) {
	target := c.baseURL + "/containers/" + url.PathEscape(idOrName) + "/json"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("docker inspect failed (%d): %s", resp.StatusCode, string(b))
	}

	var raw struct {
		ID      string `json:"Id"`
		Name    string `json:"Name"`
		Created string `json:"Created"`
		Config  struct {
			Image  string            `json:"Image"`
			Labels map[string]string `json:"Labels"`
			Env    []string          `json:"Env"`
		} `json:"Config"`
		State struct {
			Status     string `json:"Status"`
			Running    bool   `json:"Running"`
			Paused     bool   `json:"Paused"`
			Restarting bool   `json:"Restarting"`
			ExitCode   int    `json:"ExitCode"`
			Error      string `json:"Error"`
			StartedAt  string `json:"StartedAt"`
			FinishedAt string `json:"FinishedAt"`
		} `json:"State"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	return &ContainerInspect{
		ID:      raw.ID,
		Name:    raw.Name,
		Image:   raw.Config.Image,
		Created: raw.Created,
		Labels:  raw.Config.Labels,
		Config: ContainerConfig{
			Image:  raw.Config.Image,
			Labels: raw.Config.Labels,
			Env:    raw.Config.Env,
		},
		State: ContainerState{
			Status:     raw.State.Status,
			Running:    raw.State.Running,
			Paused:     raw.State.Paused,
			Restarting: raw.State.Restarting,
			ExitCode:   raw.State.ExitCode,
			Error:      raw.State.Error,
			StartedAt:  raw.State.StartedAt,
			FinishedAt: raw.State.FinishedAt,
		},
	}, nil
}

func (c *SocketDockerClient) Start(ctx context.Context, idOrName string) error {
	target := c.baseURL + "/containers/" + url.PathEscape(idOrName) + "/start"
	return c.postAction(ctx, target)
}

func (c *SocketDockerClient) Stop(ctx context.Context, idOrName string, timeoutSeconds int) error {
	target := c.baseURL + "/containers/" + url.PathEscape(idOrName) + "/stop?t=" + strconv.Itoa(timeoutSeconds)
	return c.postAction(ctx, target)
}

func (c *SocketDockerClient) Restart(ctx context.Context, idOrName string, timeoutSeconds int) error {
	target := c.baseURL + "/containers/" + url.PathEscape(idOrName) + "/restart?t=" + strconv.Itoa(timeoutSeconds)
	return c.postAction(ctx, target)
}

func (c *SocketDockerClient) postAction(ctx context.Context, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		return nil
	}

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("docker action failed (%d): %s", resp.StatusCode, string(b))
}

func (c *SocketDockerClient) Logs(ctx context.Context, idOrName string, tailLines int) (string, error) {
	if tailLines <= 0 {
		tailLines = 100
	}
	target := fmt.Sprintf("%s/containers/%s/logs?stdout=1&stderr=1&tail=%d", c.baseURL, url.PathEscape(idOrName), tailLines)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("docker logs failed (%d): %s", resp.StatusCode, string(b))
	}

	return decodeDockerMultiplexedStream(resp.Body)
}

func decodeDockerMultiplexedStream(r io.Reader) (string, error) {
	var buf bytes.Buffer
	header := make([]byte, 8)

	first := true
	for {
		n, err := io.ReadFull(r, header)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if first {
				buf.Write(header[:n])
				return buf.String(), nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				buf.Write(header[:n])
				break
			}
			return buf.String(), err
		}

		if header[0] > 2 || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			buf.Write(header)
			_, _ = io.Copy(&buf, r)
			return buf.String(), nil
		}
		first = false

		frameSize := binary.BigEndian.Uint32(header[4:8])
		if frameSize == 0 {
			continue
		}
		if frameSize > 10<<20 {
			frameSize = 10 << 20
		}
		frame := make([]byte, frameSize)
		rn, rErr := io.ReadFull(r, frame)
		buf.Write(frame[:rn])
		if rErr != nil {
			if errors.Is(rErr, io.EOF) || errors.Is(rErr, io.ErrUnexpectedEOF) {
				break
			}
			return buf.String(), rErr
		}
	}
	return buf.String(), nil
}
