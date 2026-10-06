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
	"net"
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

// ClusterEntry is one monitored cluster. Tokens and passwords are never carried
// here: they live sealed in the secrets file.
type ClusterEntry struct {
	Name string `json:"name"`
	// Kind is "peer" (another PocketKafka, monitored over its web API) or
	// "kafka" (any Kafka broker, monitored as a client). Empty means "peer" so
	// registries written before kinds existed keep working.
	Kind string `json:"kind,omitempty"`
	URL  string `json:"url,omitempty"`
	// Brokers is the bootstrap list for a "kafka" entry.
	Brokers []string `json:"brokers,omitempty"`
	// SASLUser is the username for a "kafka" entry; the password is sealed
	// separately and never returned.
	SASLUser string `json:"saslUser,omitempty"`
	// SASLMechanism defaults to PLAIN when empty.
	SASLMechanism string `json:"saslMechanism,omitempty"`
}

// Cluster kinds.
const (
	clusterKindPeer  = "peer"
	clusterKindKafka = "kafka"
)

// saslSecretPrefix namespaces SASL passwords inside the sealed secret map, so
// they cannot be mistaken for a peer's bearer token.
const saslSecretPrefix = "sasl:"

// dashboardSecretPrefix namespaces delegated dashboard credentials separately
// from read-only Health tokens and Kafka SASL passwords.
const dashboardSecretPrefix = "dashboard:"

// normalizeKind maps the empty kind to its default.
func normalizeKind(kind string) string {
	if kind == "" {
		return clusterKindPeer
	}
	return kind
}

// validateKafkaEntry checks a "kafka" entry: bootstrap addresses must be
// host:port with a real port, and a username is required because a cluster that
// needs SASL has no anonymous monitoring path.
func validateKafkaEntry(e ClusterEntry) error {
	if len(e.Brokers) == 0 {
		return errors.New("kafka cluster needs at least one broker")
	}
	for _, b := range e.Brokers {
		host, port, err := net.SplitHostPort(strings.TrimSpace(b))
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("broker %q must be host:port", b)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("broker %q has an invalid port", b)
		}
	}
	if strings.TrimSpace(e.SASLUser) == "" {
		return errors.New("kafka cluster needs a SASL username")
	}
	switch strings.ToUpper(strings.TrimSpace(e.SASLMechanism)) {
	case "", "PLAIN":
	default:
		return fmt.Errorf("unsupported SASL mechanism %q", e.SASLMechanism)
	}
	return nil
}

// validateEntry applies the rule for the entry's kind.
func validateEntry(e ClusterEntry) error {
	if err := validateClusterName(e.Name); err != nil {
		return err
	}
	switch normalizeKind(e.Kind) {
	case clusterKindKafka:
		return validateKafkaEntry(e)
	case clusterKindPeer:
		if e.URL == "" {
			return errors.New("peer cluster needs a url")
		}
		return validateClusterURL(e.URL)
	default:
		return fmt.Errorf("unsupported cluster kind %q", e.Kind)
	}
}

// validateClusterURL and validateClusterName live in the config package so the
// registry applies exactly the rule startup validation enforces.
func validateClusterURL(raw string) error   { return config.ValidateClusterURL(raw) }
func validateClusterName(name string) error { return config.ValidateClusterName(name) }

// samePeerTarget reports whether a request names exactly the peer that was
// stored, which is the condition for reusing its stored token.
func samePeerTarget(stored, want ClusterEntry) bool {
	return normalizeKind(stored.Kind) == clusterKindPeer &&
		normalizeClusterURL(stored.URL) == normalizeClusterURL(want.URL)
}

// sameKafkaTarget reports whether a request names exactly the Kafka cluster
// that was stored. The broker list is compared as a set: reordering it in the
// form does not make it a different target, but changing an address does.
func sameKafkaTarget(stored, want ClusterEntry) bool {
	if normalizeKind(stored.Kind) != clusterKindKafka {
		return false
	}
	if strings.TrimSpace(stored.SASLUser) != strings.TrimSpace(want.SASLUser) {
		return false
	}
	return sameStringSet(stored.Brokers, want.Brokers)
}

func normalizeClusterURL(u string) string {
	return strings.TrimRight(strings.TrimSpace(u), "/")
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[strings.TrimSpace(v)]++
	}
	for _, v := range b {
		key := strings.TrimSpace(v)
		if seen[key] == 0 {
			return false
		}
		seen[key]--
	}
	return true
}

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
				if validateEntry(e) == nil {
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

// DashboardToken returns a peer's stored delegated dashboard token, or "".
func (s *clusterStore) DashboardToken(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens[dashboardSecretPrefix+name]
}

// HasDashboardToken reports whether a delegated dashboard token is stored.
func (s *clusterStore) HasDashboardToken(name string) bool {
	return s.DashboardToken(name) != ""
}

// SASLPassword returns the stored SASL password for a cluster, or "".
func (s *clusterStore) SASLPassword(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokens[saslSecretPrefix+name]
}

// HasSASLPassword reports whether a password is stored, without revealing it.
func (s *clusterStore) HasSASLPassword(name string) bool {
	return s.SASLPassword(name) != ""
}

// Upsert adds or replaces one cluster. A nil secret leaves the stored one alone;
// a non-nil empty secret clears it.
func (s *clusterStore) Upsert(e ClusterEntry, token, saslPassword *string) error {
	return s.upsert(e, token, saslPassword, nil)
}

func (s *clusterStore) upsert(e ClusterEntry, token, saslPassword, dashboardToken *string) error {
	e.Name = strings.TrimSpace(e.Name)
	e.Kind = normalizeKind(e.Kind)
	e.URL = strings.TrimSpace(e.URL)
	e.SASLUser = strings.TrimSpace(e.SASLUser)
	e.SASLMechanism = strings.ToUpper(strings.TrimSpace(e.SASLMechanism))
	for i := range e.Brokers {
		e.Brokers[i] = strings.TrimSpace(e.Brokers[i])
	}
	if err := validateEntry(e); err != nil {
		return err
	}
	if dashboardToken != nil && *dashboardToken != "" && e.Kind != clusterKindPeer {
		return errors.New("dashboard tokens are supported only for peer clusters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	dashboardKey := dashboardSecretPrefix + e.Name
	if e.Kind != clusterKindPeer && dashboardToken == nil && s.tokens[dashboardKey] != "" {
		return errors.New("clear the dashboard token before changing this cluster away from peer")
	}
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
	if saslPassword != nil {
		key := saslSecretPrefix + e.Name
		if *saslPassword == "" {
			delete(s.tokens, key)
		} else {
			s.tokens[key] = *saslPassword
		}
		if err := s.persistTokensLocked(); err != nil {
			return err
		}
	}
	if dashboardToken != nil {
		if *dashboardToken == "" {
			delete(s.tokens, dashboardKey)
		} else {
			s.tokens[dashboardKey] = *dashboardToken
		}
		if err := s.persistTokensLocked(); err != nil {
			return err
		}
	}
	return s.persistEntriesLocked()
}

// Remove drops a cluster and its stored secrets.
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
	delete(s.tokens, saslSecretPrefix+name)
	delete(s.tokens, dashboardSecretPrefix+name)
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
	Name string `json:"name"`
	// Kind is "self", "peer", or "kafka".
	Kind            string   `json:"kind"`
	URL             string   `json:"url"`
	Brokers         []string `json:"brokers,omitempty"`
	Self            bool     `json:"self"`
	OK              bool     `json:"ok"`
	Error           string   `json:"error,omitempty"`
	LatencyMs       int64    `json:"latencyMs"`
	ClusterID       string   `json:"clusterId,omitempty"`
	BrokerID        int32    `json:"brokerId,omitempty"`
	Version         string   `json:"version,omitempty"`
	Status          string   `json:"status,omitempty"`
	Topics          int      `json:"topics"`
	Partitions      int      `json:"partitions"`
	Groups          int      `json:"groups"`
	TotalMessages   int64    `json:"totalMessages"`
	Lag             int64    `json:"lag"`
	MaxPartitionLag int64    `json:"maxPartitionLag"`
	MessagesPerSec  float64  `json:"messagesPerSec"`
	BytesPerSec     float64  `json:"bytesPerSec"`
	DiskUsagePct    float64  `json:"diskUsagePct"`
	RateReliable    bool     `json:"rateReliable"`
	Warmup          bool     `json:"warmup"`
	Attention       int      `json:"attention"`
	HasToken        bool     `json:"hasToken"`
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

// clusterHTTPClient fetches a peer's health report. It never follows a
// redirect (a redirect is how a URL the operator registered turns into a
// request against a host nobody registered) and dials through the address
// guard in netguard.go.
var clusterHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 nil,
		DialContext:           clusterDialContext,
		TLSHandshakeTimeout:   clusterFetchTimeout,
		ResponseHeaderTimeout: clusterFetchTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   4,
	},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// fetchClusterReport fetches a peer's raw health report. The body is returned
// untouched so the UI can render a remote cluster with the same code path.
func (s *Server) fetchClusterReport(ctx context.Context, e ClusterEntry, window time.Duration, token string) ([]byte, time.Duration, error) {
	endpoint := strings.TrimRight(e.URL, "/") + "/api/v1/health/overview?window=" + fmt.Sprintf("%d", int(window.Seconds()))
	reqCtx, cancel := context.WithTimeout(ctx, clusterFetchTimeout)
	defer cancel()
	//#nosec G704 -- endpoint is a registered cluster URL (validateClusterURL) and
	// the request is issued by clusterHTTPClient, whose dialer refuses
	// link-local/metadata addresses and never follows a redirect.
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	start := time.Now()
	//#nosec G704 -- the URL is a registered cluster endpoint and
	// clusterHTTPClient dials through guardDialAddr, which refuses
	// link-local/metadata addresses after DNS resolution and never follows a
	// redirect.
	resp, err := clusterHTTPClient.Do(req)
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
		// Uniform message: the peer's body is not echoed back, so a probing
		// cluster admin cannot use this endpoint as a blind request forgery.
		return nil, latency, fmt.Errorf("peer answered HTTP %d", resp.StatusCode)
	}
	return body, latency, nil
}

// summarize turns a peer report into one overview row.
func summarize(e ClusterEntry, hasToken bool, body []byte, latency time.Duration) clusterSummary {
	row := clusterSummary{
		Name:      e.Name,
		Kind:      clusterKindPeer,
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
	row := clusterSummary{Name: "local", Kind: "self", Self: true, OK: true}
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
	if rep.Cluster.BytesPerSec != nil {
		row.BytesPerSec = *rep.Cluster.BytesPerSec
	}
	if rep.Cluster.DiskUsagePct != nil {
		row.DiskUsagePct = *rep.Cluster.DiskUsagePct
	}
	row.RateReliable = rep.Sampling.RateReliable
	row.Warmup = rep.Sampling.Warmup
	row.Attention = len(rep.Attention)
	return row
}

// summarizeExternal builds the cluster row for a Kafka cluster we observe as a
// client. The fields a client cannot see stay zero here and are named in the
// report's unavailable list; this row is a summary, not the contract.
func summarizeExternal(e ClusterEntry, rep overviewResponse) clusterSummary {
	row := clusterSummary{
		Name:            e.Name,
		Kind:            clusterKindKafka,
		Brokers:         e.Brokers,
		OK:              true,
		ClusterID:       rep.Cluster.ClusterID,
		BrokerID:        rep.Cluster.BrokerID,
		Status:          rep.Cluster.Status,
		Topics:          rep.Cluster.Topics,
		Partitions:      rep.Cluster.Partitions,
		Groups:          rep.Cluster.Groups,
		TotalMessages:   rep.Cluster.TotalMessages,
		Lag:             rep.Cluster.Lag,
		MaxPartitionLag: rep.Cluster.MaxPartitionLag,
		MessagesPerSec:  rep.Cluster.MessagesPerSec,
		RateReliable:    rep.Sampling.RateReliable,
		Warmup:          rep.Sampling.Warmup,
		Attention:       len(rep.Attention),
	}
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
			if normalizeKind(e.Kind) == clusterKindKafka {
				// A Kafka cluster is sampled, not fetched: the row comes from the
				// last read-only pass, and the first poll starts one.
				s.externalFor(e)
				report, err := s.externalOverview(e.Name, window, now)
				if err != nil {
					rows[i+1] = clusterSummary{
						Name: e.Name, Kind: clusterKindKafka, Brokers: e.Brokers,
						OK: false, Error: err.Error(),
					}
					return
				}
				if report.Cluster.Status == "unknown" {
					// Read not finished: a row, not an unreachable cluster.
					rows[i+1] = clusterSummary{
						Name: e.Name, Kind: clusterKindKafka, Brokers: e.Brokers,
						OK: true, Status: "unknown",
					}
					return
				}
				rows[i+1] = summarizeExternal(e, report)
				return
			}
			healthToken := s.clusters.Token(e.Name)
			token := s.clusters.DashboardToken(e.Name)
			if token == "" {
				token = healthToken
			}
			body, latency, err := s.fetchClusterReport(ctx, e, window, token)
			if err != nil {
				rows[i+1] = clusterSummary{Name: e.Name, Kind: clusterKindPeer, URL: e.URL, OK: false, Error: err.Error(), LatencyMs: latency.Milliseconds(), HasToken: healthToken != ""}
				return
			}
			rows[i+1] = summarize(e, healthToken != "", body, latency)
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

var externalKafkaCapabilities = []string{"cluster", "topics.read", "topic.partitions.read", "topic.messages.read", "topic.tail", "groups.read", "group.detail.read", "health.read"}

var peerTargetCapabilities = []string{
	"cluster", "topics.read", "topics.write", "topics.delete", "topic.messages.read", "topic.messages.write",
	"topic.partitions.read", "topic.truncate", "topic.compact", "topic.import", "topic.config.read", "topic.config.write", "topic.tail",
	"groups.read", "groups.delete", "group.offsets.export", "group.offsets.reset", "group.offsets.import",
	"schemas.read", "schemas.write", "acls.read", "acls.write", "logs.read", "throughput.read", "mqtt.read", "integrations.read", "config.read", "audit.read", "health.read",
}

func targetCapabilities(kind string) []string {
	switch normalizeKind(kind) {
	case clusterKindKafka:
		return append([]string(nil), externalKafkaCapabilities...)
	case clusterKindPeer:
		return append([]string(nil), peerTargetCapabilities...)
	default:
		return []string{}
	}
}

// handleClusters lists the registry. Tokens are write-only: the response only
// says whether one is stored.
func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	entries := s.clusters.List()
	type row struct {
		Name              string   `json:"name"`
		Capabilities      []string `json:"capabilities"`
		Kind              string   `json:"kind"`
		URL               string   `json:"url,omitempty"`
		Brokers           []string `json:"brokers,omitempty"`
		SASLUser          string   `json:"saslUser,omitempty"`
		SASLMechanism     string   `json:"saslMechanism,omitempty"`
		HasToken          bool     `json:"hasToken"`
		HasDashboardToken bool     `json:"hasDashboardToken"`
		HasSASLPassword   bool     `json:"hasSaslPassword"`
	}
	out := make([]row, 0, len(entries))
	for _, e := range entries {
		out = append(out, row{
			Name:              e.Name,
			Kind:              normalizeKind(e.Kind),
			URL:               e.URL,
			Brokers:           e.Brokers,
			SASLUser:          e.SASLUser,
			SASLMechanism:     e.SASLMechanism,
			HasToken:          s.clusters.HasToken(e.Name),
			HasSASLPassword:   s.clusters.HasSASLPassword(e.Name),
			HasDashboardToken: s.clusters.HasDashboardToken(e.Name),
			Capabilities:      targetCapabilities(e.Kind),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"clusters": out})
}

// handleUpsertCluster adds or updates one monitored cluster.
func (s *Server) handleUpsertCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name                string   `json:"name"`
		Kind                string   `json:"kind"`
		URL                 string   `json:"url"`
		Brokers             []string `json:"brokers"`
		SASLUser            string   `json:"saslUser"`
		SASLMechanism       string   `json:"saslMechanism"`
		SASLPassword        string   `json:"saslPassword"`
		ClearSASLPassword   bool     `json:"clearSaslPassword"`
		Token               string   `json:"token"`
		ClearToken          bool     `json:"clearToken"`
		DashboardToken      string   `json:"dashboardToken"`
		ClearDashboardToken bool     `json:"clearDashboardToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	entry := ClusterEntry{
		Name:          req.Name,
		Kind:          req.Kind,
		URL:           req.URL,
		Brokers:       req.Brokers,
		SASLUser:      req.SASLUser,
		SASLMechanism: req.SASLMechanism,
	}
	var dashboardToken *string
	var token, saslPassword *string
	switch {
	case req.ClearToken:
		empty := ""
		token = &empty
	case req.Token != "":
		token = &req.Token
	}
	switch {
	case req.ClearDashboardToken:
		empty := ""
		dashboardToken = &empty
	case req.DashboardToken != "":
		dashboardToken = &req.DashboardToken
	}
	switch {
	case req.ClearSASLPassword:
		empty := ""
		saslPassword = &empty
	case req.SASLPassword != "":
		saslPassword = &req.SASLPassword
	}
	if token != nil || saslPassword != nil || dashboardToken != nil {
		// A stored secret can only be written sealed. When the key is missing or
		// wrong this refuses the write instead of dropping the credential.
		if err := s.clusters.BoxError(); err != nil {
			writeErr(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	if err := s.clusters.upsert(entry, token, saslPassword, dashboardToken); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "cluster.upsert", entry.Name,
		fmt.Sprintf("monitored %s cluster", normalizeKind(entry.Kind)))
	writeJSON(w, http.StatusOK, map[string]any{
		"name":              entry.Name,
		"kind":              normalizeKind(entry.Kind),
		"url":               entry.URL,
		"brokers":           entry.Brokers,
		"hasToken":          s.clusters.HasToken(entry.Name),
		"hasSaslPassword":   s.clusters.HasSASLPassword(entry.Name),
		"hasDashboardToken": s.clusters.HasDashboardToken(entry.Name),
	})
}

// handleTestCluster checks a candidate cluster WITHOUT registering it, so a
// wrong URL or token is found before it is stored. An unreachable target is a
// test RESULT, not a failed request: it answers 200 with ok=false.
func (s *Server) handleTestCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name                string   `json:"name"`
		Kind                string   `json:"kind"`
		URL                 string   `json:"url"`
		Brokers             []string `json:"brokers"`
		SASLUser            string   `json:"saslUser"`
		SASLMechanism       string   `json:"saslMechanism"`
		SASLPassword        string   `json:"saslPassword"`
		Token               string   `json:"token"`
		DashboardToken      string   `json:"dashboardToken"`
		ClearDashboardToken bool     `json:"clearDashboardToken"`
		ClearToken          bool     `json:"clearToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	entry := ClusterEntry{
		Name:          req.Name,
		Kind:          req.Kind,
		URL:           req.URL,
		Brokers:       req.Brokers,
		SASLUser:      req.SASLUser,
		SASLMechanism: req.SASLMechanism,
	}
	if normalizeKind(entry.Kind) == clusterKindKafka {
		// A Kafka cluster is tested by reading it once: connect with SASL, take
		// one read-only pass, throw the client away.
		if err := validateKafkaEntry(entry); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		password := req.SASLPassword
		if password == "" && entry.Name != "" {
			// A stored password may only be used for the target it was stored
			// for. Otherwise this endpoint is a credential exfiltration gadget:
			// {"name":"prod","brokers":["attacker:9092"]} would hand the stored
			// SASL password to a host the caller chose.
			if stored, ok := s.clusters.Get(entry.Name); ok && sameKafkaTarget(stored, entry) {
				password = s.clusters.SASLPassword(entry.Name)
			} else {
				writeErr(w, http.StatusBadRequest,
					"stored credentials belong to a different broker list or SASL user: send saslPassword explicitly to test a changed target")
				return
			}
		}
		s.recordAudit(s.actorFrom(r), "cluster.test", entry.Name,
			fmt.Sprintf("tested kafka cluster %v", entry.Brokers))
		report, err := s.sampleOnce(r.Context(), entry, password)
		if err != nil {
			writeJSON(w, http.StatusOK, clusterSummary{
				Name: entry.Name, Kind: clusterKindKafka, Brokers: entry.Brokers,
				OK: false, Error: err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, summarizeExternal(entry, report))
		return
	}
	if err := validateClusterURL(req.URL); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// A stored token is only for the entry it was stored for: reusing one for a
	// caller-chosen URL would post the credential to a host the caller picked.
	healthToken, token := req.Token, req.DashboardToken
	if req.ClearToken {
		healthToken = ""
	}
	if req.ClearDashboardToken {
		token = ""
	}
	wantHealthToken := !req.ClearToken && req.Token == ""
	wantDashboard := !req.ClearDashboardToken && req.DashboardToken == ""
	if req.Name != "" && (wantHealthToken || wantDashboard) {
		stored, ok := s.clusters.Get(req.Name)
		if !ok || !samePeerTarget(stored, entry) {
			writeErr(w, http.StatusBadRequest,
				"stored credentials belong to a different URL: send the token explicitly to test a changed target")
			return
		}
		if wantHealthToken {
			healthToken = s.clusters.Token(req.Name)
		}
		if wantDashboard {
			token = s.clusters.DashboardToken(req.Name)
		}
	}
	if token == "" {
		token = healthToken
	}
	s.recordAudit(s.actorFrom(r), "cluster.test", req.Name,
		fmt.Sprintf("tested peer cluster %s (hasToken=%v)", req.URL, healthToken != ""))
	body, latency, err := s.fetchClusterReport(r.Context(), entry, defaultWindow, token)
	if err != nil {
		writeJSON(w, http.StatusOK, clusterSummary{
			Name: req.Name, URL: req.URL, OK: false, Error: err.Error(),
			LatencyMs: latency.Milliseconds(), HasToken: healthToken != "",
		})
		return
	}
	writeJSON(w, http.StatusOK, summarize(entry, healthToken != "", body, latency))
}

// handleDeleteCluster removes one monitored cluster.
func (s *Server) handleDeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.clusters.Remove(name); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	// Stop sampling a Kafka cluster that is no longer registered: its
	// connections and goroutine go with the entry.
	s.dropExternal(name)
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
