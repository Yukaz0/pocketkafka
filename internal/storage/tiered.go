package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/tier"
)

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

func (s *Store) attach(p *Partition) {
	if s.s3 != nil {
		p.remoteFetch = s.fetchRemote
		p.remoteDelete = s.deleteRemote
	}
}

func (s *Store) fetchRemote(key string) ([]byte, error) {
	if s.s3 == nil {
		return nil, fmt.Errorf("tiered storage not enabled")
	}
	return s.s3.GetObject(key)
}

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

func (s *Store) offloadAll() {
	for _, t := range s.TopicsSnapshot() {
		for _, p := range t.Partitions {
			s.offloadPartition(p)
		}
	}
}

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

func (p *Partition) hasClosedSegmentLocked(seg *Segment) bool {
	for _, s := range p.closedSegments {
		if s == seg {
			return true
		}
	}
	return false
}

func (s *Store) tierKey(p *Partition, seg *Segment, ext string) string {
	return filepath.Join(p.topic, fmt.Sprintf("%d", p.partitionID), fmt.Sprintf("%020d%s", seg.baseOffset, ext))
}
