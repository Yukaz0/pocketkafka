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
// be restored on demand, and a deleter so retention can drop their objects.
func (s *Store) attach(p *Partition) {
	if s.s3 != nil {
		p.remoteFetch = s.fetchRemote
		p.remoteDelete = s.deleteRemote
	}
}

// fetchRemote downloads an object key from the configured store.
func (s *Store) fetchRemote(key string) ([]byte, error) {
	if s.s3 == nil {
		return nil, fmt.Errorf("tiered storage not enabled")
	}
	return s.s3.GetObject(key)
}

// deleteRemote removes an offloaded segment's log and index objects. Both are
// attempted; the first failure is returned so the caller can ignore it.
func (s *Store) deleteRemote(logKey, indexKey string) error {
	if s.s3 == nil {
		return fmt.Errorf("tiered storage not enabled")
	}
	var firstErr error
	for _, key := range []string{logKey, indexKey} {
		if err := s.s3.DeleteObject(key); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// offloadAll moves old closed segments of every partition to object storage.
func (s *Store) offloadAll() {
	for _, t := range s.TopicsSnapshot() {
		for _, p := range t.Partitions {
			s.offloadPartition(p)
		}
	}
}

// offloadPartition uploads closed segments older than the threshold. It picks
// candidates under the partition lock, does the reads and uploads without it
// (so produce/fetch are not stalled behind S3), then re-takes the lock to mark
// only the segments that are still present and not already remote.
func (s *Store) offloadPartition(p *Partition) {
	if s.s3 == nil {
		return
	}

	cutoff := time.Now().Add(-s.tierThreshold)
	p.mu.Lock()
	var candidates []*Segment
	for _, seg := range p.closedSegments {
		if seg.isRemote() {
			continue
		}
		mtime, err := seg.modTime()
		if err != nil || !mtime.Before(cutoff) {
			continue
		}
		candidates = append(candidates, seg)
	}
	p.mu.Unlock()

	type uploaded struct {
		seg      *Segment
		logKey   string
		indexKey string
	}
	var uploadedSegs []uploaded
	for _, seg := range candidates {
		logData, err := os.ReadFile(seg.logPath())
		if err != nil {
			continue
		}
		indexData, err := os.ReadFile(seg.indexPath())
		if err != nil {
			continue
		}
		logKey := s.tierKey(p, seg, ".log")
		indexKey := s.tierKey(p, seg, ".index")
		if err := s.s3.PutObject(logKey, logData); err != nil {
			continue
		}
		if err := s.s3.PutObject(indexKey, indexData); err != nil {
			continue
		}
		uploadedSegs = append(uploadedSegs, uploaded{seg: seg, logKey: logKey, indexKey: indexKey})
	}
	if len(uploadedSegs) == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, u := range uploadedSegs {
		if !p.hasClosedSegmentLocked(u.seg) || u.seg.isRemote() {
			continue
		}
		_ = u.seg.markRemote(u.logKey, u.indexKey)
	}
}

// hasClosedSegmentLocked reports whether seg is still one of the partition's
// closed segments.
func (p *Partition) hasClosedSegmentLocked(seg *Segment) bool {
	for _, s := range p.closedSegments {
		if s == seg {
			return true
		}
	}
	return false
}

// tierKey builds the object-store key for a segment file.
func (s *Store) tierKey(p *Partition, seg *Segment, ext string) string {
	return filepath.Join(p.topic, fmt.Sprintf("%d", p.partitionID), fmt.Sprintf("%020d%s", seg.baseOffset, ext))
}
