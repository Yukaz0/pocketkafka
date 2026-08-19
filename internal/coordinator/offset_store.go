package coordinator

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
)

// CommittedOffset is the persisted position for one group/topic/partition.
type CommittedOffset struct {
	Offset      int64
	Metadata    *string
	LeaderEpoch int32
}

// OffsetStore persists committed consumer group offsets. It supports an
// in-memory backend and a file-backed backend (gob encoded on disk) so offsets
// survive broker restarts.
type OffsetStore struct {
	mu      sync.RWMutex
	backend string
	path    string
	offsets map[string]map[string]map[int32]*CommittedOffset // group -> topic -> partition
}

// NewOffsetStore creates an offset store. backend may be "inmemory" for a pure
// in-memory store, or any other value (e.g. "pebble") for a file-backed store.
func NewOffsetStore(backend, dir string) (*OffsetStore, error) {
	s := &OffsetStore{
		backend: backend,
		offsets: make(map[string]map[string]map[int32]*CommittedOffset),
	}
	if backend != "inmemory" {
		s.path = filepath.Join(dir, "offsets.gob")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		if err := s.load(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Commit stores an offset for a group/topic/partition.
func (s *OffsetStore) Commit(group, topic string, partition int32, off *CommittedOffset) error {
	s.mu.Lock()
	gt, ok := s.offsets[group]
	if !ok {
		gt = make(map[string]map[int32]*CommittedOffset)
		s.offsets[group] = gt
	}
	tp, ok := gt[topic]
	if !ok {
		tp = make(map[int32]*CommittedOffset)
		gt[topic] = tp
	}
	tp[partition] = off
	s.mu.Unlock()

	if s.path != "" {
		return s.save()
	}
	return nil
}

// Fetch returns the committed offset for a group/topic/partition, or nil.
func (s *OffsetStore) Fetch(group, topic string, partition int32) (*CommittedOffset, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	gt, ok := s.offsets[group]
	if !ok {
		return nil, false
	}
	tp, ok := gt[topic]
	if !ok {
		return nil, false
	}
	off, ok := tp[partition]
	return off, ok
}

// GroupTopics returns the set of topic names with commits for a group.
func (s *OffsetStore) GroupTopics(group string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	gt, ok := s.offsets[group]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(gt))
	for t := range gt {
		out = append(out, t)
	}
	return out
}

// Partitions returns committed partitions for a group/topic.
func (s *OffsetStore) Partitions(group, topic string) []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	gt, ok := s.offsets[group]
	if !ok {
		return nil
	}
	tp, ok := gt[topic]
	if !ok {
		return nil
	}
	out := make([]int32, 0, len(tp))
	for p := range tp {
		out = append(out, p)
	}
	return out
}

// GroupOffsets returns all committed offsets for a group as topic -> partition
// -> offset.
func (s *OffsetStore) GroupOffsets(group string) map[string]map[int32]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	gt, ok := s.offsets[group]
	if !ok {
		return nil
	}
	out := make(map[string]map[int32]int64, len(gt))
	for topic, tp := range gt {
		parts := make(map[int32]int64, len(tp))
		for p, off := range tp {
			parts[p] = off.Offset
		}
		out[topic] = parts
	}
	return out
}

func (s *OffsetStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	return dec.Decode(&s.offsets)
}

func (s *OffsetStore) save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(s.offsets); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
