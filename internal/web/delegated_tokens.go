package web

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
	"github.com/Yukaz0/pocketkafka/internal/authz"
)

const delegatedTokensFile = "__dashboard_tokens.json"

var errUnknownDelegatedToken = errors.New("unknown delegated token")

type delegatedTokenRecord struct {
	ID        string `json:"id"`
	Principal string `json:"principal"`
	Digest    string `json:"digest"`
}

type delegatedTokenInfo struct {
	ID        string `json:"id"`
	Principal string `json:"principal"`
}

type delegatedTokenStore struct {
	mu       sync.RWMutex
	path     string
	records  map[string]delegatedTokenRecord
	byDigest map[string]string
	loadErr  error
}

func newDelegatedTokenStore(path string) *delegatedTokenStore {
	s := &delegatedTokenStore{
		path:     path,
		records:  make(map[string]delegatedTokenRecord),
		byDigest: make(map[string]string),
	}
	if path == "" {
		return s
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s
	}
	if err != nil {
		s.loadErr = fmt.Errorf("read delegated tokens: %w", err)
		return s
	}
	var records []delegatedTokenRecord
	if err := json.Unmarshal(data, &records); err != nil {
		s.loadErr = fmt.Errorf("decode delegated tokens: %w", err)
		return s
	}
	for _, record := range records {
		decoded, err := hex.DecodeString(record.Digest)
		if record.ID == "" || record.Principal == "" || err != nil || len(decoded) != sha256.Size {
			s.loadErr = errors.New("invalid delegated token record")
			s.records = make(map[string]delegatedTokenRecord)
			s.byDigest = make(map[string]string)
			return s
		}
		if _, exists := s.records[record.ID]; exists {
			s.loadErr = errors.New("duplicate delegated token ID")
			s.records = make(map[string]delegatedTokenRecord)
			s.byDigest = make(map[string]string)
			return s
		}
		if _, exists := s.byDigest[record.Digest]; exists {
			s.loadErr = errors.New("duplicate delegated token digest")
			s.records = make(map[string]delegatedTokenRecord)
			s.byDigest = make(map[string]string)
			return s
		}
		s.records[record.ID] = record
		s.byDigest[record.Digest] = record.ID
	}
	return s
}

func (s *delegatedTokenStore) Err() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.loadErr
}

func (s *delegatedTokenStore) Issue(principal string) (id, raw string, err error) {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return "", "", errors.New("principal is required")
	}
	var entropy [48]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", "", fmt.Errorf("generate delegated token: %w", err)
	}
	id = "dt_" + base64.RawURLEncoding.EncodeToString(entropy[:16])
	raw = "pka_" + base64.RawURLEncoding.EncodeToString(entropy[16:])
	digest := sha256.Sum256([]byte(raw))
	record := delegatedTokenRecord{ID: id, Principal: principal, Digest: hex.EncodeToString(digest[:])}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return "", "", s.loadErr
	}
	if _, exists := s.records[id]; exists {
		return "", "", errors.New("delegated token ID collision")
	}
	s.records[id] = record
	s.byDigest[record.Digest] = id
	if err := s.persistLocked(); err != nil {
		delete(s.records, id)
		delete(s.byDigest, record.Digest)
		return "", "", err
	}
	return id, raw, nil
}

func (s *delegatedTokenStore) Principal(raw string) (string, bool) {
	digest := sha256.Sum256([]byte(raw))
	key := hex.EncodeToString(digest[:])
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.loadErr != nil {
		return "", false
	}
	id, ok := s.byDigest[key]
	if !ok {
		return "", false
	}
	record, ok := s.records[id]
	return record.Principal, ok
}

func (s *delegatedTokenStore) List() []delegatedTokenInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.loadErr != nil {
		return nil
	}
	out := make([]delegatedTokenInfo, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, delegatedTokenInfo{ID: record.ID, Principal: record.Principal})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *delegatedTokenStore) Revoke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	record, ok := s.records[id]
	if !ok {
		return errUnknownDelegatedToken
	}
	delete(s.records, id)
	delete(s.byDigest, record.Digest)
	if err := s.persistLocked(); err != nil {
		s.records[id] = record
		s.byDigest[record.Digest] = id
		return err
	}
	return nil
}

func (s *delegatedTokenStore) persistLocked() error {
	if s.path == "" {
		return nil
	}
	records := make([]delegatedTokenRecord, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicfile.Write(s.path, data, 0o600); err != nil {
		return fmt.Errorf("persist delegated tokens: %w", err)
	}
	return nil
}

func (s *Server) requireDelegatedTokenAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !s.authEnabled || !s.authorize || s.aclStore == nil {
		writeErr(w, http.StatusForbidden, "delegated tokens require security and cluster Admin permission")
		return false
	}
	principal := s.requestPrincipal(r)
	if principal == "" {
		writeErr(w, http.StatusUnauthorized, "authentication required")
		return false
	}
	resource := authz.Resource{Type: authz.ResourceCluster, Name: "cluster"}
	if err := s.aclStore.Authorize(principal, authz.OpAdmin, resource); err != nil {
		writeErr(w, http.StatusForbidden, "forbidden: "+err.Error())
		return false
	}
	return true
}

func (s *Server) delegatedTokenStoreReady(w http.ResponseWriter) bool {
	if s.delegatedTokens == nil || s.delegatedTokens.Err() != nil {
		writeErr(w, http.StatusServiceUnavailable, "delegated token store unavailable")
		return false
	}
	return true
}

func (s *Server) handleListDelegatedTokens(w http.ResponseWriter, r *http.Request) {
	if !s.requireDelegatedTokenAdmin(w, r) || !s.delegatedTokenStoreReady(w) {
		return
	}
	writeJSON(w, http.StatusOK, s.delegatedTokens.List())
}

func (s *Server) handleCreateDelegatedToken(w http.ResponseWriter, r *http.Request) {
	if !s.requireDelegatedTokenAdmin(w, r) || !s.delegatedTokenStoreReady(w) {
		return
	}
	var req struct {
		Principal string `json:"principal"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	principal := strings.TrimSpace(req.Principal)
	if !userExists(s.users, principal) {
		writeErr(w, http.StatusBadRequest, "principal must name a configured user")
		return
	}
	id, token, err := s.delegatedTokens.Issue(principal)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not issue delegated token")
		return
	}
	s.recordAudit(s.requestPrincipal(r), "auth.delegated_token.create", id, "issued delegated token for "+principal)
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "token": token})
}

func (s *Server) handleRevokeDelegatedToken(w http.ResponseWriter, r *http.Request) {
	if !s.requireDelegatedTokenAdmin(w, r) || !s.delegatedTokenStoreReady(w) {
		return
	}
	id := r.PathValue("id")
	if err := s.delegatedTokens.Revoke(id); err != nil {
		if errors.Is(err, errUnknownDelegatedToken) {
			writeErr(w, http.StatusNotFound, "delegated token not found")
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "could not revoke delegated token")
		return
	}
	s.recordAudit(s.requestPrincipal(r), "auth.delegated_token.revoke", id, "revoked delegated token")
	writeJSON(w, http.StatusOK, map[string]string{"revoked": id})
}
