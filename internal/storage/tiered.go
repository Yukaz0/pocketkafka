package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/tier"
)

// EnableTiering activates S3/MinIO tiered storage: closed segments older than
// threshold are offloaded to the object store and fetched back on demand. It
// returns a stop function for the background offload loop.
func (s *Store) EnableTiering(client *tier.S3Client, threshold, interval time.Duration) func() {
	s.s3 = client
	s.tierThreshold = threshold
	for _, t := range s.TopicsSnapshot() {
		for _, p := range t.Partitions {
			s.attach(p)
		}
	}
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				s.offloadAll()
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}

// attach wires an object-store fetcher onto a partition so remote segments can
// be restored on demand.
func (s *Store) attach(p *Partition) {
	if s.s3 != nil {
		p.remoteFetch = s.fetchRemote
	}
}

// fetchRemote downloads an object key from the configured store.
func (s *Store) fetchRemote(key string) ([]byte, error) {
	if s.s3 == nil {
		return nil, fmt.Errorf("tiered storage not enabled")
	}
	return s.s3.GetObject(key)
}

// offloadAll moves old closed segments of every partition to object storage.
func (s *Store) offloadAll() {
	for _, t := range s.TopicsSnapshot() {
		for _, p := range t.Partitions {
			s.offloadPartition(p)
		}
	}
}

// offloadPartition uploads closed segments older than the threshold.
func (s *Store) offloadPartition(p *Partition) {
	if s.s3 == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-s.tierThreshold)
	kept := p.closedSegments[:0]
	for _, seg := range p.closedSegments {
		if seg.isRemote() {
			kept = append(kept, seg)
			continue
		}
		mtime, err := fileMtime(seg.logPath())
		if err != nil || !mtime.Before(cutoff) {
			kept = append(kept, seg)
			continue
		}
		logData, err := os.ReadFile(seg.logPath())
		if err != nil {
			kept = append(kept, seg)
			continue
		}
		indexData, err := os.ReadFile(seg.indexPath())
		if err != nil {
			kept = append(kept, seg)
			continue
		}
		logKey := s.tierKey(p, seg, ".log")
		indexKey := s.tierKey(p, seg, ".index")
		if err := s.s3.PutObject(logKey, logData); err != nil {
			kept = append(kept, seg)
			continue
		}
		if err := s.s3.PutObject(indexKey, indexData); err != nil {
			kept = append(kept, seg)
			continue
		}
		if err := seg.markRemote(logKey, indexKey); err != nil {
			kept = append(kept, seg)
			continue
		}
	}
	p.closedSegments = kept
}

// tierKey builds the object-store key for a segment file.
func (s *Store) tierKey(p *Partition, seg *Segment, ext string) string {
	return filepath.Join(p.topic, fmt.Sprintf("%d", p.partitionID), fmt.Sprintf("%020d%s", seg.baseOffset, ext))
}
