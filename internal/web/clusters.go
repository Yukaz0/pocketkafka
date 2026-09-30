package web

// Multi-cluster monitoring: the dashboard of one broker reports the health of
// others, driven by a registry that only ever fetches URLs it contains, so the
// browser cannot turn the fan-out into an SSRF primitive.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
	"github.com/Yukaz0/pocketkafka/internal/config"
)

const (
	clustersFileName  = "__clusters.json"
	clusterTokensFile = "__cluster_tokens.json"
	// secretsKeyFile seals stored tokens; losing it makes them unreadable.
	secretsKeyFile = "__secrets.key"
	// clusterFetchTimeout bounds one remote fetch; a dead peer must not stall
	// the whole overview.
	clusterFetchTimeout = 4 * time.Second
	// clusterSummaryTTL keeps a polling dashboard from hammering every peer.
	clusterSummaryTTL = 2 * time.Second
	// clusterMaxBody caps a remote response we are willing to buffer.
	clusterMaxBody = 8 << 20
)

// ClusterEntry is one monitored broker. Tokens are never carried here.
type ClusterEntry struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// validateClusterURL and validateClusterName live in the config package so the
// registry applies exactly the rule startup validation enforces.
func validateClusterURL(raw string) error   { return config.ValidateClusterURL(raw) }
func validateClusterName(name string) error { return config.ValidateClusterName(name) }

// clusterStore owns the registry and its two files.
type clusterStore struct {
	mu          sync.Mutex
	entries     []ClusterEntry
	tokens      map[string]string
	entriesPath string
	tokensPath  string
	persistent  bool
	// box seals tokens before they are written. It is nil only when there is
	// nothing to persist: refusing to store a token beats storing it in the clear.
	box *secretBox
	// boxErr records a secrets problem so the next write reports it instead of
	// failing silently.
	boxErr error
}

// newClusterStore loads the registry. The config seed is applied only when no
// registry file exists yet, so a cluster removed from the UI does not reappear
// on the next restart.
func newClusterStore(dataDir string, seed []config.WebCluster, box *secretBox) *clusterStore {
	s := &clusterStore{tokens: map[string]string{}, box: box}
	if dataDir == "" {
		for _, c := range seed {
			s.entries = append(s.entries, ClusterEntry{Name: c.Name, URL: c.URL})
		}
		return s
	}
	s.persistent = true
	s.entriesPath = filepath.Join(dataDir, clustersFileName)
	s.tokensPath = filepath.Join(dataDir, clusterTokensFile)

	if b, err := os.ReadFile(s.tokensPath); err == nil {
		var stored map[string]string
		if json.Unmarshal(b, &stored) == nil {
			needsMigration := false
			for name, v := range stored {
				if v == "" {
					continue
				}
				if s.box == nil {
					if strings.HasPrefix(v, secretPrefix) {
						// Sealed without a key: keeping the ciphertext as a
						// token would send garbage to the peer.
						s.boxErr = errors.New("secrets key unavailable: stored cluster tokens cannot be read")
						continue
					}
					s.tokens[name] = v
					needsMigration = true
					continue
				}
				plain, wasPlain, err := s.box.open(v)
				if err != nil {
					s.boxErr = fmt.Errorf("cluster token %q: %w", name, err)
					continue
				}
				s.tokens[name] = plain
				needsMigration = needsMigration || wasPlain
			}
			if needsMigration {
				// A file written before encryption existed is rewritten sealed
				// right away, so the migration does not depend on somebody
				// editing the registry later.
				if err := s.persistTokensLocked(); err != nil && s.boxErr == nil {
					s.boxErr = err
				}
			}
		}
	}
	if b, err := os.ReadFile(s.entriesPath); err == nil {
		var list []ClusterEntry
		if json.Unmarshal(b, &list) == nil {
			for _, e := range list {
				if validateClusterName(e.Name) == nil && validateClusterURL(e.URL) == nil {
					s.entries = append(s.entries, e)
				}
			}
			return s
		}
	}
	for _, c := range seed {
		if validateClusterName(c.Name) != nil || validateClusterURL(c.URL) != nil {
			continue
		}
		s.entries = append(s.entries, ClusterEntry{Name: c.Name, URL: c.URL})
	}
	if len(s.entries) > 0 {
		_ = s.persistEntriesLocked()
	}
	return s
}

// List returns a copy of the registry, sorted by name.
func (s *clusterStore) List() []ClusterEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]ClusterEntry(nil), s.entries...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Get returns one entry by name.
func (s *clusterStore) Get(name string) (ClusterEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.Name == name {
			return e, true
		}
	}
	return ClusterEntry{}, false
}

// Token returns the stored bearer token for a cluster, or "".
func (s *clusterStore) Token(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens[name]
}

// HasToken reports whether a token is stored, without revealing it.
func (s *clusterStore) HasToken(name string) bool {
	return s.Token(name) != ""
}

// Upsert adds or replaces one cluster. A nil token leaves the stored token
// alone; a non-nil empty token clears it.
func (s *clusterStore) Upsert(e ClusterEntry, token *string) error {
	if err := validateClusterName(e.Name); err != nil {
		return err
	}
	if err := validateClusterURL(e.URL); err != nil {
		return err
	}
	e.Name = strings.TrimSpace(e.Name)
	e.URL = strings.TrimSpace(e.URL)

	s.mu.Lock()
	defer s.mu.Unlock()
	replaced := false
	for i := range s.entries {
		if s.entries[i].Name == e.Name {
			s.entries[i] = e
			replaced = true
			break
		}
	}
	if !replaced {
		s.entries = append(s.entries, e)
	}
	if token != nil {
		if *token == "" {
			delete(s.tokens, e.Name)
		} else {
			s.tokens[e.Name] = *token
		}
		if err := s.persistTokensLocked(); err != nil {
			return err
		}
	}
	return s.persistEntriesLocked()
}

// Remove drops a cluster and its token.
func (s *clusterStore) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.entries[:0]
	found := false
	for _, e := range s.entries {
		if e.Name == name {
			found = true
			continue
		}
		kept = append(kept, e)
	}
	if !found {
		return errors.New("unknown cluster")
	}
	s.entries = kept
	delete(s.tokens, name)
	if err := s.persistTokensLocked(); err != nil {
		return err
	}
	return s.persistEntriesLocked()
}

func (s *clusterStore) persistEntriesLocked() error {
	if !s.persistent {
		return nil
	}
	data, err := json.MarshalIndent(s.entries, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(s.entriesPath, data, 0o600)
}

func (s *clusterStore) persistTokensLocked() error {
	if !s.persistent {
		return nil
	}
	if s.box == nil {
		if len(s.tokens) == 0 {
			return nil
		}
		return errors.New("secrets key unavailable: refusing to write cluster tokens unencrypted")
	}
	sealed := make(map[string]string, len(s.tokens))
	for name, v := range s.tokens {
		enc, err := s.box.seal(v)
		if err != nil {
			return err
		}
		sealed[name] = enc
	}
	data, err := json.MarshalIndent(sealed, "", "  ")
	if err != nil {
		return err
	}
	return atomicfile.Write(s.tokensPath, data, 0o600)
}

// BoxError reports a secrets problem that would make the next token write fail.
func (s *clusterStore) BoxError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boxErr
}

// Fan-out

// clusterSummary is one row of the cluster overview. It carries the few fields
// an operator scans, not the whole report.
type clusterSummary struct {
	Name            string  `json:"name"`
	URL             string  `json:"url"`
	Self            bool    `json:"self"`
	OK              bool    `json:"ok"`
	Error           string  `json:"error,omitempty"`
	LatencyMs       int64   `json:"latencyMs"`
	ClusterID       string  `json:"clusterId,omitempty"`
	BrokerID        int32   `json:"brokerId,omitempty"`
	Version         string  `json:"version,omitempty"`
	Status          string  `json:"status,omitempty"`
	Topics          int     `json:"topics"`
	Partitions      int     `json:"partitions"`
	Groups          int     `json:"groups"`
	TotalMessages   int64   `json:"totalMessages"`
	Lag             int64   `json:"lag"`
	MaxPartitionLag int64   `json:"maxPartitionLag"`
	MessagesPerSec  float64 `json:"messagesPerSec"`
	BytesPerSec     float64 `json:"bytesPerSec"`
	DiskUsagePct    float64 `json:"diskUsagePct"`
	RateReliable    bool    `json:"rateReliable"`
	Warmup          bool    `json:"warmup"`
	Attention       int     `json:"attention"`
	HasToken        bool    `json:"hasToken"`
}

// remoteReport is the subset of a health report the fan-out reads. Unknown
// fields are ignored, so a newer peer cannot break an older aggregator.
type remoteReport struct {
	Cluster struct {
		ClusterID       string  `json:"clusterId"`
		BrokerID        int32   `json:"brokerId"`
		Version         string  `json:"version"`
		Status          string  `json:"status"`
		Topics          int     `json:"topics"`
		Partitions      int     `json:"partitions"`
		Groups          int     `json:"groups"`
		TotalMessages   int64   `json:"totalMessages"`
		Lag             int64   `json:"lag"`
		MaxPartitionLag int64   `json:"maxPartitionLag"`
		MessagesPerSec  float64 `json:"messagesPerSec"`
		BytesPerSec     float64 `json:"bytesPerSec"`
		DiskUsagePct    float64 `json:"diskUsagePct"`
	} `json:"cluster"`
	Sampling struct {
		RateReliable bool `json:"rateReliable"`
		Warmup       bool `json:"warmup"`
	} `json:"sampling"`
	Attention []json.RawMessage `json:"attention"`
}

// fetchClusterReport fetches a peer's raw health report. The body is returned
// untouched so the UI can render a remote cluster with the same code path.
func (s *Server) fetchClusterReport(ctx context.Context, e ClusterEntry, window time.Duration, token string) ([]byte, time.Duration, error) {
	endpoint := strings.TrimRight(e.URL, "/") + "/api/v1/health/overview?window=" + fmt.Sprintf("%d", int(window.Seconds()))
	reqCtx, cancel := context.WithTimeout(ctx, clusterFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	latency := time.Since(start)
	if err != nil {
		return nil, latency, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, clusterMaxBody))
	if err != nil {
		return nil, latency, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, latency, fmt.Errorf("peer answered HTTP %d", resp.StatusCode)
	}
	return body, latency, nil
}

// summarize turns a peer report into one overview row.
func summarize(e ClusterEntry, hasToken bool, body []byte, latency time.Duration) clusterSummary {
	row := clusterSummary{
		Name:      e.Name,
		URL:       e.URL,
		OK:        true,
		LatencyMs: latency.Milliseconds(),
		HasToken:  hasToken,
	}
	var rep remoteReport
	if err := json.Unmarshal(body, &rep); err != nil {
		row.OK = false
		row.Error = "peer returned invalid JSON"
		return row
	}
	row.ClusterID = rep.Cluster.ClusterID
	row.BrokerID = rep.Cluster.BrokerID
	row.Version = rep.Cluster.Version
	row.Status = rep.Cluster.Status
	row.Topics = rep.Cluster.Topics
	row.Partitions = rep.Cluster.Partitions
	row.Groups = rep.Cluster.Groups
	row.TotalMessages = rep.Cluster.TotalMessages
	row.Lag = rep.Cluster.Lag
	row.MaxPartitionLag = rep.Cluster.MaxPartitionLag
	row.MessagesPerSec = rep.Cluster.MessagesPerSec
	row.BytesPerSec = rep.Cluster.BytesPerSec
	row.DiskUsagePct = rep.Cluster.DiskUsagePct
	row.RateReliable = rep.Sampling.RateReliable
	row.Warmup = rep.Sampling.Warmup
	row.Attention = len(rep.Attention)
	if row.Status == "" {
		row.OK = false
		row.Error = "peer report has no cluster status"
	}
	return row
}

// selfSummary reports this broker, so one screen shows every cluster including
// the one serving the dashboard.
func (s *Server) selfSummary(window time.Duration, now time.Time) clusterSummary {
	row := clusterSummary{Name: "local", Self: true, OK: true}
	if s.store == nil {
		row.OK = false
		row.Error = "storage unavailable"
		return row
	}
	rep := s.buildOverview(defaultThresholds(), window, now)
	row.ClusterID = rep.Cluster.ClusterID
	row.BrokerID = rep.Cluster.BrokerID
	row.Version = rep.Cluster.Version
	row.Status = rep.Cluster.Status
	row.Topics = rep.Cluster.Topics
	row.Partitions = rep.Cluster.Partitions
	row.Groups = rep.Cluster.Groups
	row.TotalMessages = rep.Cluster.TotalMessages
	row.Lag = rep.Cluster.Lag
	row.MaxPartitionLag = rep.Cluster.MaxPartitionLag
	row.MessagesPerSec = rep.Cluster.MessagesPerSec
	row.BytesPerSec = rep.Cluster.BytesPerSec
	row.DiskUsagePct = rep.Cluster.DiskUsagePct
	row.RateReliable = rep.Sampling.RateReliable
	row.Warmup = rep.Sampling.Warmup
	row.Attention = len(rep.Attention)
	return row
}

// clustersOverview fans out to every registered cluster. A dead peer becomes a
// row with ok=false and its error, never a failed request: monitoring must
// survive the thing it monitors.
func (s *Server) clustersOverview(ctx context.Context, window time.Duration, now time.Time) map[string]any {
	entries := s.clusters.List()
	rows := make([]clusterSummary, len(entries)+1)
	rows[0] = s.selfSummary(window, now)

	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, e := range entries {
		wg.Add(1)
		go func(i int, e ClusterEntry) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			token := s.clusters.Token(e.Name)
			body, latency, err := s.fetchClusterReport(ctx, e, window, token)
			if err != nil {
				rows[i+1] = clusterSummary{Name: e.Name, URL: e.URL, OK: false, Error: err.Error(), LatencyMs: latency.Milliseconds(), HasToken: token != ""}
				return
			}
			rows[i+1] = summarize(e, token != "", body, latency)
		}(i, e)
	}
	wg.Wait()

	worst := "healthy"
	unreachable := 0
	for _, r := range rows {
		if !r.OK {
			unreachable++
			continue
		}
		if statusRank(r.Status) > statusRank(worst) {
			worst = r.Status
		}
	}
	return map[string]any{
		"generatedAtMs": now.UnixMilli(),
		"windowSeconds": int(window.Seconds()),
		"clusters":      rows,
		"worst":         worst,
		"unreachable":   unreachable,
	}
}

// HTTP

// handleClustersOverview serves one row per monitored cluster, including this
// broker. Results are cached briefly so a 2s UI poll does not fan out at that
// rate.
func (s *Server) handleClustersOverview(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	window := defaultWindow
	if v := r.URL.Query().Get("window"); v != "" {
		if n, err := parseWindow(v); err == nil {
			window = n
		}
	}
	now := time.Now()
	s.monitor.Record(s.store, s.gm, now, false)

	s.clusterCacheMu.Lock()
	cached, ok := s.clusterCache[window]
	s.clusterCacheMu.Unlock()
	if ok && now.Sub(cached.at) < clusterSummaryTTL {
		writeJSON(w, http.StatusOK, cached.payload)
		return
	}

	payload := s.clustersOverview(r.Context(), window, now)
	s.clusterCacheMu.Lock()
	if s.clusterCache == nil {
		s.clusterCache = map[time.Duration]clusterCacheEntry{}
	}
	s.clusterCache[window] = clusterCacheEntry{at: now, payload: payload}
	s.clusterCacheMu.Unlock()
	writeJSON(w, http.StatusOK, payload)
}

// handleClusters lists the registry. Tokens are write-only: the response only
// says whether one is stored.
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	entries := s.clusters.List()
	type row struct {
		Name     string `json:"name"`
		URL      string `json:"url"`
		HasToken bool   `json:"hasToken"`
	}
	out := make([]row, 0, len(entries))
	for _, e := range entries {
		out = append(out, row{Name: e.Name, URL: e.URL, HasToken: s.clusters.HasToken(e.Name)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": out})
}

// handleUpsertCluster adds or updates one monitored cluster.
func (s *Server) handleUpsertCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		URL        string `json:"url"`
		Token      string `json:"token"`
		ClearToken bool   `json:"clearToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	entry := ClusterEntry{Name: req.Name, URL: req.URL}
	var token *string
	switch {
	case req.ClearToken:
		empty := ""
		token = &empty
	case req.Token != "":
		token = &req.Token
	}
	if token != nil {
		// A stored token can only be written sealed. When the key is missing or
		// wrong this refuses the write instead of dropping the credential.
		if err := s.clusters.BoxError(); err != nil {
			writeErr(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	if err := s.clusters.Upsert(entry, token); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "cluster.upsert", entry.Name, "monitored cluster "+entry.URL)
	writeJSON(w, http.StatusOK, map[string]any{"name": entry.Name, "url": entry.URL, "hasToken": s.clusters.HasToken(entry.Name)})
}

// handleTestCluster checks a candidate cluster WITHOUT registering it, so a
// wrong URL or token is found before it is stored. An unreachable target is a
// test RESULT, not a failed request: it answers 200 with ok=false.
func (s *Server) handleTestCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string `json:"name"`
		URL   string `json:"url"`
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := validateClusterURL(req.URL); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	token := req.Token
	if token == "" && req.Name != "" {
		// The edit form leaves the token empty to keep the stored one, so the
		// test must use that stored token instead of probing unauthenticated.
		token = s.clusters.Token(req.Name)
	}
	entry := ClusterEntry{Name: req.Name, URL: req.URL}
	body, latency, err := s.fetchClusterReport(r.Context(), entry, defaultWindow, token)
	if err != nil {
		writeJSON(w, http.StatusOK, clusterSummary{
			Name: req.Name, URL: req.URL, OK: false, Error: err.Error(),
			LatencyMs: latency.Milliseconds(), HasToken: token != "",
		})
		return
	}
	writeJSON(w, http.StatusOK, summarize(entry, token != "", body, latency))
}

// handleDeleteCluster removes one monitored cluster.
func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.clusters.Remove(name); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "cluster.delete", name, "removed monitored cluster")
	writeJSON(w, http.StatusOK, map[string]string{"deleted": name})
}

// parseWindow clamps a window query parameter.
func parseWindow(v string) (time.Duration, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n < 5 || n > 3600 {
		return 0, errors.New("window must be 5..3600 seconds")
	}
	return time.Duration(n) * time.Second, nil
}

// clusterCacheEntry memoizes one fan-out result.
type clusterCacheEntry struct {
	at      time.Time
	payload map[string]any
}

// WithClusterMonitoring installs the multi-cluster registry: the config seed,
// the accept-side bearer token, the key that seals stored tokens, and the
// persistence paths under the data dir.
func (s *Server) WithClusterMonitoring(cfg config.Config) *Server {
	s.clusterToken = cfg.Web.ClusterToken
	box, err := loadClusterSecrets(cfg, s.dataDir)
	s.clusters = newClusterStore(s.dataDir, cfg.Web.Clusters, box)
	if err != nil {
		s.clusters.boxErr = err
	}
	return s
}

// loadClusterSecrets returns the box that seals stored tokens. An in-memory
// registry persists nothing and needs no key; with a data dir the key is
// required, and its absence is reported rather than worked around.
func loadClusterSecrets(cfg config.Config, dataDir string) (*secretBox, error) {
	if dataDir == "" {
		return nil, nil
	}
	key := cfg.Web.SecretsKey
	if key == "" {
		key = os.Getenv(secretsKeyEnv)
	}
	return newSecretBox(key, filepath.Join(dataDir, secretsKeyFile))
}
