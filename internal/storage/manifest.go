package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// topicManifest persists per-topic configuration (e.g. cleanup.policy=compact)
// across broker restarts.
type topicManifest struct {
	Compacted []string `json:"compacted"`
}

func (s *Store) manifestPath() string {
	return filepath.Join(s.dir, "__topics.json")
}

// loadManifest reads the topic manifest if present.
func (s *Store) loadManifest() error {
	data, err := os.ReadFile(s.manifestPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var m topicManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for _, name := range m.Compacted {
		s.compacted[name] = true
	}
	return nil
}

// saveManifest persists the current topic configuration.
func (s *Store) saveManifest() error {
	m := topicManifest{}
	for name, c := range s.compacted {
		if c {
			m.Compacted = append(m.Compacted, name)
		}
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := s.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.manifestPath())
}

// IsCompacted reports whether a topic uses log compaction.
func (s *Store) IsCompacted(topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compacted[topic]
}

// MarkCompacted toggles log compaction for a topic and persists the setting.
func (s *Store) MarkCompacted(topic string, compact bool) error {
	s.mu.Lock()
	if compact {
		s.compacted[topic] = true
	} else {
		delete(s.compacted, topic)
	}
	err := s.saveManifest()
	s.mu.Unlock()
	return err
}
