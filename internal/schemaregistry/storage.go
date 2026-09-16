package schemaregistry

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
)

// persistedEntry is one schema version as stored on disk.
type persistedEntry struct {
	ID         int    `json:"id"`
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

// persistedState is the whole registry document.
type persistedState struct {
	NextID  int              `json:"nextId"`
	Entries []persistedEntry `json:"entries"`
}

// loadState reads the registry file. A missing file returns (nil, nil).
func loadState(path string) (*persistedState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("schemaregistry: read %s: %w", path, err)
	}
	if len(data) == 0 {
		return &persistedState{NextID: 1}, nil
	}
	var st persistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("schemaregistry: parse %s: %w", path, err)
	}
	return &st, nil
}

// saveState atomically replaces the registry file.
func saveState(path string, st persistedState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("schemaregistry: marshal: %w", err)
	}
	return atomicfile.Write(path, data, 0o600)
}
