package coordinator

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
)

// Offset store backend names (kept in sync with config.BackendFile /
// config.BackendInMemory; duplicated here so the coordinator package does not
// depend on the config package).
const (
	BackendFile     = "file"
	BackendInMemory = "inmemory"
)

// walCompactThreshold is the number of WAL records appended before the store
// rewrites the snapshot and truncates the WAL. It bounds replay time on restart.
const walCompactThreshold = 1024

// CommittedOffset is the persisted position for one group/topic/partition.
type CommittedOffset struct {
	Offset      int64
	Metadata    *string
	LeaderEpoch int32
	// UpdatedAt is the Unix-nano time of the last commit, used for offset
	// retention. Zero means "unknown" (loaded from a legacy snapshot).
	UpdatedAt int64
}

// OffsetStore persists committed consumer group offsets. It supports an
// in-memory backend and a file-backed backend (a gob snapshot plus a
// write-ahead log) so offsets survive broker restarts without rewriting the
// whole snapshot on every commit.
type OffsetStore struct {
	mu      sync.RWMutex
	backend string
	path    string // snapshot path (empty for in-memory)
	wal     *offsetWAL
	walMu   sync.Mutex
	walN    int

	offsets map[string]map[string]map[int32]*CommittedOffset // group -> topic -> partition
}

// NewOffsetStore creates an offset store. backend is "inmemory" for a pure
// in-memory store or "file" for the snapshot+WAL store persisted under dir. Any
// other value is rejected; in particular the legacy "pebble" spelling is
// normalized to "file" by the config layer before it reaches here, because the
// implementation has never used Pebble.
func NewOffsetStore(backend, dir string) (*OffsetStore, error) {
	s := &OffsetStore{
		backend: backend,
		offsets: make(map[string]map[string]map[int32]*CommittedOffset),
	}
	switch backend {
	case BackendInMemory:
		// Nothing to load.
	case BackendFile:
		s.path = filepath.Join(dir, "offsets.gob")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		if err := s.loadSnapshot(); err != nil {
			return nil, err
		}
		wal, err := openWAL(filepath.Join(dir, "offsets.wal"))
		if err != nil {
			return nil, err
		}
		s.wal = wal
		if err := s.replayWAL(); err != nil {
			wal.close()
			return nil, err
		}
		// Collapse a long WAL into a fresh snapshot on startup.
		if s.walN >= walCompactThreshold {
			if err := s.compact(); err != nil {
				wal.close()
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("unknown offset store backend %q", backend)
	}
	return s, nil
}

// Commit stores an offset for a group/topic/partition and logs it to the WAL.
func (s *OffsetStore) Commit(group, topic string, partition int32, off *CommittedOffset) error {
	if off.UpdatedAt == 0 {
		off.UpdatedAt = time.Now().UnixNano()
	}
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

	if s.wal == nil {
		return nil
	}
	return s.appendAndMaybeCompact(walRecord{
		Type:        walRecordCommit,
		Group:       group,
		Topic:       topic,
		Partition:   partition,
		Offset:      off.Offset,
		LeaderEpoch: off.LeaderEpoch,
		Metadata:    off.Metadata,
		Timestamp:   off.UpdatedAt,
	})
}

func (s *OffsetStore) appendAndMaybeCompact(rec walRecord) error {
	s.walMu.Lock()
	if err := s.wal.append(rec); err != nil {
		s.walMu.Unlock()
		return err
	}
	s.walN++
	compact := s.walN >= walCompactThreshold
	s.walMu.Unlock()
	if compact {
		return s.compact()
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

// Groups returns the names of all groups with committed offsets.
func (s *OffsetStore) Groups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.offsets))
	for g := range s.offsets {
		out = append(out, g)
	}
	return out
}

// PruneOlderThan removes committed offsets last updated before cutoff and
// persists the result. It returns the number of offsets removed. An offset with
// a zero UpdatedAt (unknown, e.g. migrated from a legacy snapshot) is treated as
// expired only when cutoff is non-zero, matching the "retention applies from
// now on" contract. The caller supplies now so tests can inject a clock.
func (s *OffsetStore) PruneOlderThan(cutoff time.Time) int {
	if cutoff.IsZero() {
		return 0
	}
	cut := cutoff.UnixNano()
	removed := 0

	s.mu.Lock()
	for group, topics := range s.offsets {
		for topic, parts := range topics {
			for part, off := range parts {
				if off.UpdatedAt < cut {
					delete(parts, part)
					removed++
				}
			}
			if len(parts) == 0 {
				delete(topics, topic)
			}
		}
		if len(topics) == 0 {
			delete(s.offsets, group)
		}
	}
	s.mu.Unlock()

	if removed > 0 && s.wal != nil {
		_ = s.compact()
	}
	return removed
}

// Close flushes a final snapshot and closes the WAL.
func (s *OffsetStore) Close() error {
	if s.wal == nil {
		return nil
	}
	err := s.compact()
	if cerr := s.wal.close(); err == nil {
		err = cerr
	}
	return err
}

func (s *OffsetStore) loadSnapshot() error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&s.offsets); err != nil {
		return fmt.Errorf("offset snapshot %s: %w", s.path, err)
	}
	return nil
}

func (s *OffsetStore) replayWAL() error {
	if s.wal == nil {
		return nil
	}
	n := 0
	err := s.wal.replay(func(rec walRecord) {
		if rec.Type != walRecordCommit {
			return
		}
		gt, ok := s.offsets[rec.Group]
		if !ok {
			gt = make(map[string]map[int32]*CommittedOffset)
			s.offsets[rec.Group] = gt
		}
		tp, ok := gt[rec.Topic]
		if !ok {
			tp = make(map[int32]*CommittedOffset)
			gt[rec.Topic] = tp
		}
		tp[rec.Partition] = &CommittedOffset{
			Offset:      rec.Offset,
			Metadata:    rec.Metadata,
			LeaderEpoch: rec.LeaderEpoch,
			UpdatedAt:   rec.Timestamp,
		}
		n++
	})
	if err != nil {
		return err
	}
	s.walN = n
	return nil
}

// compact writes a fresh snapshot and truncates the WAL. Callers must not hold
// s.mu.
func (s *OffsetStore) compact() error {
	if s.path == "" {
		return nil
	}
	s.mu.RLock()
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(s.offsets)
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := atomicfile.Write(s.path, buf.Bytes(), 0o600); err != nil {
		return err
	}
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if err := s.wal.reset(); err != nil {
		return err
	}
	s.walN = 0
	return nil
}
