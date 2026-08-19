// Package schemaregistry implements an embedded Schema Registry with a
// Confluent-compatible REST API, so standard clients (kafka-avro-serializer,
// confluent-kafka-go) can use it without a separate container.
package schemaregistry

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type schemaEntry struct {
	ID         int
	Subject    string
	Version    int
	Schema     string
	SchemaType string
}

// Registry is an in-memory schema store.
type Registry struct {
	mu        sync.RWMutex
	nextID    int
	bySubject map[string][]*schemaEntry // subject -> versions (ascending)
	byID      map[int]*schemaEntry
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{
		nextID:    1,
		bySubject: make(map[string][]*schemaEntry),
		byID:      make(map[int]*schemaEntry),
	}
}

// Handler returns the HTTP handler exposing the Confluent-compatible API.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /subjects", r.listSubjects)
	mux.HandleFunc("POST /subjects/{subject}/versions", r.register)
	mux.HandleFunc("GET /subjects/{subject}/versions", r.listVersions)
	mux.HandleFunc("GET /subjects/{subject}/versions/{version}", r.getVersion)
	mux.HandleFunc("GET /schemas/ids/{id}", r.getByID)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (r *Registry) listSubjects(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	subjects := make([]string, 0, len(r.bySubject))
	for s := range r.bySubject {
		subjects = append(subjects, s)
	}
	r.mu.RUnlock()
	sort.Strings(subjects)
	if subjects == nil {
		subjects = []string{}
	}
	writeJSON(w, 200, subjects)
}

func (r *Registry) register(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	var body struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil || body.Schema == "" {
		writeJSON(w, 422, map[string]string{"error_code": "422", "message": "schema required"})
		return
	}
	schemaType := strings.ToUpper(body.SchemaType)
	if schemaType == "" {
		schemaType = "AVRO"
	}

	r.mu.Lock()
	// Deduplicate: if the exact schema already exists for the subject, return it.
	for _, e := range r.bySubject[subject] {
		if e.Schema == body.Schema && e.SchemaType == schemaType {
			id := e.ID
			r.mu.Unlock()
			writeJSON(w, 200, map[string]int{"id": id})
			return
		}
	}
	id := r.nextID
	r.nextID++
	version := len(r.bySubject[subject]) + 1
	entry := &schemaEntry{ID: id, Subject: subject, Version: version, Schema: body.Schema, SchemaType: schemaType}
	r.bySubject[subject] = append(r.bySubject[subject], entry)
	r.byID[id] = entry
	r.mu.Unlock()

	writeJSON(w, 200, map[string]int{"id": id})
}

func (r *Registry) listVersions(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	r.mu.RLock()
	entries := r.bySubject[subject]
	r.mu.RUnlock()
	if len(entries) == 0 {
		writeJSON(w, 404, map[string]string{"error_code": "40401", "message": "subject not found"})
		return
	}
	versions := make([]int, 0, len(entries))
	for _, e := range entries {
		versions = append(versions, e.Version)
	}
	writeJSON(w, 200, versions)
}

func (r *Registry) getVersion(w http.ResponseWriter, req *http.Request) {
	subject := req.PathValue("subject")
	ver := req.PathValue("version")
	r.mu.RLock()
	entries := r.bySubject[subject]
	r.mu.RUnlock()
	if len(entries) == 0 {
		writeJSON(w, 404, map[string]string{"error_code": "40401", "message": "subject not found"})
		return
	}
	idx := -1
	if ver == "latest" {
		idx = len(entries) - 1
	} else if v, err := strconv.Atoi(ver); err == nil {
		for i, e := range entries {
			if e.Version == v {
				idx = i
				break
			}
		}
	}
	if idx < 0 || idx >= len(entries) {
		writeJSON(w, 404, map[string]string{"error_code": "40402", "message": "version not found"})
		return
	}
	e := entries[idx]
	writeJSON(w, 200, map[string]interface{}{
		"subject":    e.Subject,
		"version":    e.Version,
		"id":         e.ID,
		"schema":     e.Schema,
		"schemaType": e.SchemaType,
	})
}

func (r *Registry) getByID(w http.ResponseWriter, req *http.Request) {
	id, _ := strconv.Atoi(req.PathValue("id"))
	r.mu.RLock()
	e := r.byID[id]
	r.mu.RUnlock()
	if e == nil {
		writeJSON(w, 404, map[string]string{"error_code": "40403", "message": "schema id not found"})
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"schema":     e.Schema,
		"schemaType": e.SchemaType,
	})
}
