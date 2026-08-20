package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/tier"
)

// Topic groups the partitions that make up one logical topic.
type Topic struct {
	Name       string
	Partitions map[int32]*Partition
}

// Store owns all topics on disk and coordinates partition lifecycle. It is the
// central handle exposed to the protocol layer.
type Store struct {
	mu              sync.RWMutex
	dir             string
	topics          map[string]*Topic
	compacted       map[string]bool // topics with cleanup.policy=compact
	maxSegmentBytes int64
	indexInterval   int64
	s3              *tier.S3Client
	tierThreshold   time.Duration
}

// NewStore opens (or creates) the data directory and recovers existing topics.
func NewStore(dir string, maxSegmentBytes, indexInterval int64) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	s := &Store{
		dir:             dir,
		topics:          make(map[string]*Topic),
		compacted:       make(map[string]bool),
		maxSegmentBytes: maxSegmentBytes,
		indexInterval:   indexInterval,
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	if err := s.loadManifest(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover loads any topics that already exist in the data directory.
func (s *Store) recover() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		topic := e.Name()
		t := &Topic{Name: topic, Partitions: make(map[int32]*Partition)}
		partitions, err := os.ReadDir(filepath.Join(s.dir, topic))
		if err != nil {
			return err
		}
		for _, pe := range partitions {
			pid, err := strconv.ParseInt(pe.Name(), 10, 32)
			if err != nil || !pe.IsDir() {
				continue
			}
			p, err := OpenPartition(filepath.Join(s.dir, topic, pe.Name()), topic, int32(pid), s.maxSegmentBytes, s.indexInterval)
			if err != nil {
				return err
			}
			s.attach(p)
			t.Partitions[int32(pid)] = p
		}
		if len(t.Partitions) > 0 {
			s.topics[topic] = t
		}
	}
	return nil
}

// partitionDir returns the on-disk directory for a topic-partition.
func (s *Store) partitionDir(topic string, partition int32) string {
	return filepath.Join(s.dir, topic, fmt.Sprintf("%d", partition))
}

// ValidateTopicName checks that a topic name is safe to use as a directory name
// and conforms to reasonable Kafka constraints.
func ValidateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name is empty")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("invalid topic name %q", name)
	}
	if len(name) > 249 {
		return fmt.Errorf("topic name too long")
	}
	for _, c := range name {
		if c == '/' || c == '\\' || c == ':' || c == ' ' || c < 0x20 || c == 0x7f {
			return fmt.Errorf("invalid character %q in topic name", c)
		}
	}
	return nil
}

// EnsureTopic creates a topic with the given partition count if it does not
// already exist. If it exists, it is returned unchanged.
func (s *Store) EnsureTopic(name string, partitions int) (*Topic, bool, error) {
	if err := ValidateTopicName(name); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if partitions <= 0 {
		partitions = 1
	}
	if t, ok := s.topics[name]; ok {
		return t, false, nil
	}
	return s.createLocked(name, partitions)
}

// CreateTopic creates a brand new topic, erroring if it already exists.
func (s *Store) CreateTopic(name string, partitions int) (*Topic, error) {
	if err := ValidateTopicName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[name]; ok {
		return nil, fmt.Errorf("topic already exists: %s", name)
	}
	if partitions <= 0 {
		partitions = 1
	}
	t, _, err := s.createLocked(name, partitions)
	return t, err
}

func (s *Store) createLocked(name string, partitions int) (*Topic, bool, error) {
	t := &Topic{Name: name, Partitions: make(map[int32]*Partition)}
	for i := 0; i < partitions; i++ {
		p, err := OpenPartition(s.partitionDir(name, int32(i)), name, int32(i), s.maxSegmentBytes, s.indexInterval)
		if err != nil {
			// Best-effort cleanup of already created partitions.
			for _, cp := range t.Partitions {
				cp.Delete()
			}
			return nil, false, err
		}
		s.attach(p)
		t.Partitions[int32(i)] = p
	}
	s.topics[name] = t
	return t, true, nil
}

// DeleteTopic removes a topic and all of its partition data from disk.
func (s *Store) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[name]
	if !ok {
		return fmt.Errorf("unknown topic: %s", name)
	}
	for _, p := range t.Partitions {
		if err := p.Delete(); err != nil {
			return err
		}
	}
	delete(s.topics, name)
	return nil
}

// GetPartition returns the partition for a topic, or nil if it does not exist.
func (s *Store) GetPartition(topic string, partition int32) *Partition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[topic]
	if !ok {
		return nil
	}
	return t.Partitions[partition]
}

// GetTopic returns a topic by name, or nil.
func (s *Store) GetTopic(name string) *Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[name]
}

// TopicNames returns the sorted list of topic names.
func (s *Store) TopicNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// TopicsSnapshot returns a shallow copy of the topic map for iteration.
func (s *Store) TopicsSnapshot() map[string]*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*Topic, len(s.topics))
	for k, v := range s.topics {
		out[k] = v
	}
	return out
}

// PartitionCount returns the number of partitions for a topic (or 0 if unknown).
func (s *Store) PartitionCount(topic string) int {
	t := s.GetTopic(topic)
	if t == nil {
		return 0
	}
	return len(t.Partitions)
}

// CompactNow runs log compaction immediately for a topic marked
// cleanup.policy=compact. It is the backend for the web UI compact action.
func (s *Store) CompactNow(topic string) error {
	t := s.GetTopic(topic)
	if t == nil {
		return fmt.Errorf("unknown topic: %s", topic)
	}
	for _, p := range t.Partitions {
		if err := p.Compact(); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all open partition segment files.
func (s *Store) Close() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var firstErr error
	for _, t := range s.topics {
		for _, p := range t.Partitions {
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// DiskUsagePct reports the percentage of the data directory's filesystem that
// is used, for the web UI health alarm (0-100). It returns 0 if the stat call
// fails (e.g. unsupported platform), so the UI stays quiet.
func (s *Store) DiskUsagePct() float64 {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(s.dir, &fs); err != nil {
		return 0
	}
	if fs.Blocks == 0 {
		return 0
	}
	used := float64(fs.Blocks - fs.Bfree)
	return used / float64(fs.Blocks) * 100
}
