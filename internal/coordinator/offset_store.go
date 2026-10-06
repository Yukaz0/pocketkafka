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
	// compacting guards against overlapping background compactions; closed
	// stops new ones from starting so Close can drain them.
	compacting bool
	closed     bool
	compactWG  sync.WaitGroup

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
		if err := os.MkdirAll(dir, 0o750); err != nil {
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

// OffsetCommitRecord is one offset to persist in a batched commit.
type OffsetCommitRecord struct {
	Group     string
	Topic     string
	Partition int32
	Offset    *CommittedOffset
}

// Commit stores an offset for a group/topic/partition and logs it to the WAL.
func (s *OffsetStore) Commit(group, topic string, partition int32, off *CommittedOffset) error {
	return s.CommitBatch([]OffsetCommitRecord{{Group: group, Topic: topic, Partition: partition, Offset: off}})
}

// CommitBatch applies several committed offsets to memory and appends them to
// the WAL with a single fsync, so an OffsetCommit request touching N partitions
// pays one fsync instead of N.
func (s *OffsetStore) CommitBatch(records []OffsetCommitRecord) error {
	if len(records) == 0 {
		return nil
	}
	now := time.Now().UnixNano()
	recs := make([]walRecord, 0, len(records))
	s.mu.Lock()
	for _, r := range records {
		off := r.Offset
		if off.UpdatedAt == 0 {
			off.UpdatedAt = now
		}
		gt, ok := s.offsets[r.Group]
		if !ok {
			gt = make(map[string]map[int32]*CommittedOffset)
			s.offsets[r.Group] = gt
		}
		tp, ok := gt[r.Topic]
		if !ok {
			tp = make(map[int32]*CommittedOffset)
			gt[r.Topic] = tp
		}
		tp[r.Partition] = off
		recs = append(recs, walRecord{
			Type:        walRecordCommit,
			Group:       r.Group,
			Topic:       r.Topic,
			Partition:   r.Partition,
			Offset:      off.Offset,
			LeaderEpoch: off.LeaderEpoch,
			Metadata:    off.Metadata,
			Timestamp:   off.UpdatedAt,
		})
	}
	s.mu.Unlock()

	if s.wal == nil {
		return nil
	}
	return s.appendAndMaybeCompact(recs)
}

// appendAndMaybeCompact appends the records and, when the WAL has grown past the
// compaction threshold, starts a background compaction. Compaction holds walMu
// across the snapshot encode and the WAL reset, which is what keeps an
// acknowledged commit from being truncated away; running it in the background
// means the caller's group lock is not held for the duration of the I/O.
func (s *OffsetStore) appendAndMaybeCompact(recs []walRecord) error {
	s.walMu.Lock()
	if err := s.wal.appendBatch(recs); err != nil {
		s.walMu.Unlock()
		return err
	}
	s.walN += len(recs)
	trigger := s.walN >= walCompactThreshold && !s.compacting && !s.closed
	if trigger {
		s.compacting = true
		s.compactWG.Add(1)
	}
	s.walMu.Unlock()

	if trigger {
		go s.compactAsync()
	}
	return nil
}

func (s *OffsetStore) compactAsync() {
	defer s.compactWG.Done()
	_ = s.compact()
	s.walMu.Lock()
	s.compacting = false
	s.walMu.Unlock()
}

// DeleteGroup removes every committed offset for a group and persists the
// result, matching Kafka's DeleteGroups semantics.
func (s *OffsetStore) DeleteGroup(group string) error {
	s.mu.Lock()
	_, ok := s.offsets[group]
	delete(s.offsets, group)
	s.mu.Unlock()
	if !ok || s.wal == nil {
		return nil
	}
	return s.compact()
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
	// Stop new background compactions and wait for any in-flight one before
	// touching the WAL, so compact() and close() cannot race it.
	s.walMu.Lock()
	s.closed = true
	s.walMu.Unlock()
	s.compactWG.Wait()

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
// s.mu or walMu.
func (s *OffsetStore) compact() error {
	s.walMu.Lock()
	defer s.walMu.Unlock()
	return s.compactLocked()
}

// compactLocked writes a fresh snapshot and truncates the WAL. The caller must
// hold walMu, which keeps the snapshot encode and the reset atomic with respect
// to concurrent commits. It takes s.mu.RLock to encode; commits are designed to
// release s.mu before acquiring walMu, so the lock order is safe.
func (s *OffsetStore) compactLocked() error {
	if s.path == "" {
		s.walN = 0
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
	if err := s.wal.reset(); err != nil {
		return err
	}
	s.walN = 0
	return nil
}
